# SEDE.proxy

This project contains the implementation of a simple http server that sits in front of multiple SEDE executors in a local network and serves as a proxy.

## Build

Build a runnable jar using gradle:

`.gradlew shadowJar`

## Download

Download the runnable jar from [here](https://github.com/aminfa/SEDE.proxy/releases/download/v2.0/ExecutorProxy-2.0.jar).

## Run

Run while providing a port as the first argument:

`java -jar ExecutorProxy-2.0.jar 8080`


