package com.reactive.exception;

public class NetworkException extends RuntimeException {
    String message;

    public NetworkException(String message) {
        super(message);
        this.message = message;
    }
}
