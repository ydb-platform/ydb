package ru.yandex.passport.tvmauth.exception;

public class PermissionDenied extends NonRetriableException {
    public PermissionDenied(String message) {
        super(message);
    }
}
