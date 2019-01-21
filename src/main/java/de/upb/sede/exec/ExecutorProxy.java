package de.upb.sede.exec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A HTTP Server with a mapping from executor-id to http address.
 * This server has the following contexts:
 *
 *   -  POST; url=/signup/[executor_id]/[executor_address] ; [executor_id] and [executor_address] are `application/x-www-form-urlencoded` encoded.
 *                                                          This method will map the executor_id to the executor_address.
 *   -  GET; url=/mapping ; This method returns the current executor mappings in json format, e.g.: {"executor_id1" : "executor_address_1", "executor_id2": "executor_address_2"}
 *
 *   - POST/GET/PUT; url=/[.*]/[executor_id]/[.*] ; This method forwards the request to the address looked up in the mapping. Returns 400 if the mapping doesn't contain the executor_id.
 */
public class ExecutorProxy {

    private final static Logger logger = LoggerFactory.getLogger(ExecutorProxy.class);

    private final Map<String, String> proxyMapping = new HashMap<>();
    private final HttpServer server;
    private final int port;

    private final static String URL_ENCODING =  "UTF-8";

    private final static String SIGNUP_HANDLE = "/signup/";
    private final static String GET_MAPPING_HANDLE = "/mapping";

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final ThreadPoolExecutor service;

    /**
     * This constructor creates a http server with the given port.
     * @param port http port valued between 1 and 65535
     */
    public ExecutorProxy(int port) {
        this.port = port;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.service = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        server.setExecutor(service);
        server.createContext(SIGNUP_HANDLE, new SignUpHandler());
        server.createContext(GET_MAPPING_HANDLE, new GetMappingHandler());
        server.createContext("/", new ForwardToExecutor());
    }

    /**
     * Maps the given executorid to the given executoraddress.
     * @param executorId id of executor behind this proxy.
     * @param executorAddress local address of the executor behind this proxy.
     */
    private synchronized void signup(String executorId, String executorAddress) {
        logger.info("Mapping `{}` to `{}`.", executorId, executorAddress);
        this.proxyMapping.put(executorId, executorAddress);
    }


    /**
     * Handles executor signups
     */
    class SignUpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpExchange.getResponseHeaders().add("Content-Type", "text/plain");
                httpExchange.getResponseHeaders().add("Content-Type", "charset=UTF-8");
                httpExchange.getRequestBody().close();
                if("GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                    httpExchange.sendResponseHeaders(405, 0);
                    httpExchange.getResponseBody().close();
                    return;
                }
                String path = httpExchange.getRequestURI().getPath();
                if(!path.startsWith(SIGNUP_HANDLE)) {
                    logger.error("The http path `" + path + "` doesn't start with: " + SIGNUP_HANDLE);
                    httpExchange.sendResponseHeaders(400, 0);
                    httpExchange.getResponseBody().close();
                    return;
                }
                String[] mapping = path.substring(SIGNUP_HANDLE.length()).split("/");
                if(mapping.length != 2) {
                    logger.error("Cannot decode the signup url `" + path + "` mapping.");
                    httpExchange.sendResponseHeaders(400, 0);
                    httpExchange.getResponseBody().close();
                    return;
                }
                String executorId = URLDecoder.decode(mapping[0], URL_ENCODING);
                String executorAddress = URLDecoder.decode(mapping[1], URL_ENCODING);
                ExecutorProxy.this.signup(executorId, executorAddress);
                httpExchange.sendResponseHeaders(200, 0);
                httpExchange.getResponseBody().close();
            } catch (RuntimeException ex) {
                String requester = httpExchange.getRemoteAddress().getHostName();
                String port = "" + httpExchange.getRemoteAddress().getPort();
                String url = httpExchange.getRequestURI().getPath();
                logger.error("Error handle of request " + url + " from entity " + requester + ":" + port, ex);
            }
        }
    }

    /**
     * Serializes the mapping into json.
     * @return serializes java mapping.
     */
    private synchronized String mappingJsonSerialization() {
        return gson.toJson(proxyMapping);
    }

    /**
     * Handles getting the map.
     */
    private class GetMappingHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                logger.info("Current mappings requested.");
                httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpExchange.getResponseHeaders().add("Content-Type", "text/plain");
                httpExchange.getResponseHeaders().add("Content-Type", "charset=UTF-8");
                byte[] returnBody = mappingJsonSerialization().getBytes(URL_ENCODING);
                httpExchange.sendResponseHeaders(200, returnBody.length);
                httpExchange.getResponseBody().write(returnBody);
                httpExchange.getResponseBody().close();
            } catch (RuntimeException ex) {
                String requester = httpExchange.getRemoteAddress().getHostName();
                String port = "" + httpExchange.getRemoteAddress().getPort();
                String url = httpExchange.getRequestURI().getPath();
                logger.error("Error handle of request " + url + " from entity " + requester + ":" + port + "\n", ex);
            }
        }
    }

    private synchronized Optional<String> getMapping(String executorId) {
        return Optional.ofNullable(proxyMapping.get(executorId));
    }



    private class ForwardToExecutor implements HttpHandler {
        @Override
        public void handle(HttpExchange source) throws IOException {
            String uri = source.getRequestURI().getPath().substring(1); // drop the first "/"
            logger.debug("Forward request received {}: url=`{}`", source.getRequestMethod(), uri);
            int firstSlashPosition = uri.indexOf("/");
            int secondSlashPosition = uri.indexOf("/", uri.indexOf("/") + 1);
            if(firstSlashPosition == -1) {
                logger.error("Cannot forward url=`{}`. Doesn't contain executorid as the second field.", uri);
                source.sendResponseHeaders(404, 0);
                source.getResponseBody().close();
                return;
            }
            if(secondSlashPosition == -1) {
                // no second slash. consider the rest of the uri as the executor id
                secondSlashPosition = uri.length();
            }
            String executorId = uri.substring(firstSlashPosition + 1, secondSlashPosition);
            Optional<String> internalAddress = getMapping(executorId);
            if(internalAddress.isPresent()) {
                try{
                    /*
                     * Prepare client connection to sink:
                     */
                    URL url = new URL("http://"+ internalAddress.get() + "/" + uri);
                    logger.info("Forwarding to `{}`.", url.toString());
                    HttpURLConnection sink = (HttpURLConnection) url.openConnection();
                    sink.setDoInput(true);
                    sink.setDoOutput(true);
                    sink.setRequestMethod(source.getRequestMethod());
                    for(Map.Entry<String, List<String>> sourceHeader : source.getRequestHeaders().entrySet()){
                        for(String value : sourceHeader.getValue()) {
                            logger.trace("Forwarding header: {}={}", sourceHeader.getKey(), value );
                            sink.setRequestProperty(sourceHeader.getKey(), value);
                        }
                    }

                    /*
                     * Write request body into sink
                     */
                    ExtendedByteArrayOutputStream payload = new ExtendedByteArrayOutputStream();
                    copy(source.getRequestBody(), payload);
                    sink.setRequestProperty("content-length", "" + payload.size());
                    sink.setFixedLengthStreamingMode(payload.size());
                    sink.setConnectTimeout(8000);
                    sink.connect();
                    copy(payload.toInputStream(), sink.getOutputStream());

                    /*
                     * Write return body into source
                     */
                    int sink_ResponceCode = sink.getResponseCode();

//                    payload = new ExtendedByteArrayOutputStream();
//                    IOUtils.copy(sink.getInputStream(), payload);
                    for(Map.Entry<String, List<String>> returnHeader : sink.getHeaderFields().entrySet()) {
                        if(returnHeader == null || returnHeader.getKey() == null || returnHeader.getValue() == null) {
                            continue;
                        }
                        if(returnHeader.getKey().equalsIgnoreCase("content-length") || returnHeader.getKey().equalsIgnoreCase("Transfer-Encoding")) {
                            continue; // skip content-length specification from sink:
                        }
                        logger.debug("Return header: {}={}", returnHeader.getKey(), returnHeader.getValue());
                        source.getResponseHeaders().put(returnHeader.getKey(), returnHeader.getValue());
                    }
                    source.sendResponseHeaders(sink_ResponceCode, 0);
                    copy(sink.getInputStream(), source.getResponseBody());
                    sink.disconnect();
                    source.getResponseBody().close();
                    logger.debug("Forwarding finished. Current active threads: {} ", service.getActiveCount() );
                } catch (Exception ex) {
                    logger.error("Error during forward to `{}`", internalAddress.get(), ex);
                    throw ex;
                }
            } else {
                logger.error("Forward {} requested but there is no matching executor signed up. Current mapping: \n{}", uri, mappingJsonSerialization());
                source.sendResponseHeaders(404, 0);
                source.getResponseBody().close();
            }
        }
    }

    static class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {

        ExtendedByteArrayOutputStream() {
            this(2<<12);
        }

        ExtendedByteArrayOutputStream(int size) {
            super(size);
        }


        /**
         * Returns an instance of ByteArrayInputStream initialized with the data written into this stream. \n
         * It does this without copying the buffer thus the runtime of this method is O(1). \n
         *
         *
         * @return an inputstream that holds the data written onto this output stream.
         */
        public InputStream toInputStream() {
            return new ByteArrayInputStream(this.buf, 0, this.count);
        }

    }

    private void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[2<<14];
        int n;
        final int EOF = -1;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }

    public static void main(String[] args) {
        int port = 8080;
        if(args.length > 0) {
            try{
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException ex) {
                logger.error("First argument {} was assumed to be a port but cannot be casted to integer: ", ex);
            }
        }
        ExecutorProxy proxy = new ExecutorProxy(port);
        proxy.server.start();
        logger.info("Started http server with port=" + port);
    }
}
