package com.reactive.exception;

public class ServiceException extends RuntimeException {
    String message;

    public ServiceException(String message) {
        super(message);
        this.message = message;
    }
}
