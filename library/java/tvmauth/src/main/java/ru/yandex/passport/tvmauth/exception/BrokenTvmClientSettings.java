package ru.yandex.passport.tvmauth.exception;

public class BrokenTvmClientSettings extends NonRetriableException {
    public BrokenTvmClientSettings(String message) {
        super(message);
    }
}
