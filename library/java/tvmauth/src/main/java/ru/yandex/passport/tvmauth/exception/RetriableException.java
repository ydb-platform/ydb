package ru.yandex.passport.tvmauth.exception;

public class RetriableException extends ClientException {
    public RetriableException(String message) {
        super(message);
    }
}
