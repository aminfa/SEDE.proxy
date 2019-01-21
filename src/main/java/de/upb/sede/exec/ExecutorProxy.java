package de.upb.sede.exec;

import com.google.common.base.Charsets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * A HTTP Server with a mapping from executor-id to http address.
 * Delegates all calls to the
 */
public class ExecutorProxy {

    private final static Logger logger = LoggerFactory.getLogger(ExecutorProxy.class);

    private final Map<String, String> proxyMapping = new HashMap<>();
    private final HttpServer server;
    private final int port;

    private final static String URL_ENCODING =  Charsets.UTF_8.name();

    private final static String SIGNUP_HANDLE = "/signup/";
    private final static String GET_MAPPING_HANDLE = "/mapping";

    /**
     * This constructor creates a http server with the given port.
     * @param port http port valued between 1 and 65535
     */
    public ExecutorProxy(int port) {
        this.port = port;

        try {
            logger.info("Starting http server with port=" + port);
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext(SIGNUP_HANDLE, new SignUpHandler());
        server.createContext(GET_MAPPING_HANDLE, new GetMappingHandler());
    }

    private synchronized void signup(String executorId, String executorAddress) {
        this.proxyMapping.put(executorId, executorAddress);
    }


    class SignUpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpExchange.getResponseHeaders().add("Content-Type", "text/plain");
                httpExchange.getResponseHeaders().add("Content-Type", "charset=UTF-8");
                if("GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                    httpExchange.sendResponseHeaders(405, 0);
                    httpExchange.getRequestBody().close();
                    return;
                }
                String path = httpExchange.getRequestURI().getPath();
                if(!path.startsWith(SIGNUP_HANDLE)) {
                    httpExchange.sendResponseHeaders(400, 0);
                    httpExchange.getRequestBody().close();
                    throw new RuntimeException("The http path `" + path + "` doesn't start with: " + SIGNUP_HANDLE);
                }
                String[] mapping = path.substring(SIGNUP_HANDLE.length()).split("/");
                if(mapping.length != 2) {
                    httpExchange.sendResponseHeaders(400, 0);
                    httpExchange.getRequestBody().close();
                    throw new RuntimeException("The http path `" + path + "` contains a bad mapping.");
                }
                String executorId = URLDecoder.decode(mapping[0], URL_ENCODING);
                String executorAddress = URLDecoder.decode(mapping[1], URL_ENCODING);
                ExecutorProxy.this.signup(executorId, executorAddress);
                httpExchange.sendResponseHeaders(200, 0);
                httpExchange.getRequestBody().close();
            } catch (RuntimeException ex) {
                String requester = httpExchange.getRemoteAddress().getHostName();
                String port = "" + httpExchange.getRemoteAddress().getPort();
                String url = httpExchange.getRequestURI().getPath();
                logger.error("Error handle of request " + url + " from entity " + requester + ":" + port + "\n", ex);
            }
        }
    }

    private class GetMappingHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpExchange.getResponseHeaders().add("Content-Type", "text/plain");
                httpExchange.getResponseHeaders().add("Content-Type", "charset=UTF-8");
                if("GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                    httpExchange.sendResponseHeaders(405, 0);
                    httpExchange.getRequestBody().close();
                    return;
                }
                String path = httpExchange.getRequestURI().getPath();
                if(!path.startsWith(SIGNUP_HANDLE)) {
                    httpExchange.sendResponseHeaders(400, 0);
                    httpExchange.getRequestBody().close();
                    throw new RuntimeException("The http path `" + path + "` doesn't start with: " + SIGNUP_HANDLE);
                }
                String[] mapping = path.substring(SIGNUP_HANDLE.length()).split("/");
                if(mapping.length != 2) {
                    httpExchange.sendResponseHeaders(400, 0);
                    httpExchange.getRequestBody().close();
                    throw new RuntimeException("The http path `" + path + "` contains a bad mapping.");
                }
                String executorId = URLDecoder.decode(mapping[0], URL_ENCODING);
                String executorAddress = URLDecoder.decode(mapping[1], URL_ENCODING);
                ExecutorProxy.this.signup(executorId, executorAddress);
                httpExchange.sendResponseHeaders(200, 0);
                httpExchange.getRequestBody().close();
            } catch (RuntimeException ex) {
                String requester = httpExchange.getRemoteAddress().getHostName();
                String port = "" + httpExchange.getRemoteAddress().getPort();
                String url = httpExchange.getRequestURI().getPath();
                logger.error("Error handle of request " + url + " from entity " + requester + ":" + port + "\n", ex);
            }
        }
    }
}
