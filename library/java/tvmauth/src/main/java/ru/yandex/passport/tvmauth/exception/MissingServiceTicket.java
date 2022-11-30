package ru.yandex.passport.tvmauth.exception;

public class MissingServiceTicket extends NonRetriableException {
    public MissingServiceTicket(String message) {
        super(message);
    }
}
