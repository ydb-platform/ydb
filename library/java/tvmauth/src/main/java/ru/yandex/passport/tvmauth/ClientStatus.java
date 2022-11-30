package ru.yandex.passport.tvmauth;

import javax.annotation.Nonnull;

public class ClientStatus {
    private Code code;
    private String lastError;

    public enum Code {
        OK,
        WARNING,
        ERROR,
    }

    public ClientStatus(@Nonnull Code code, @Nonnull String lastError) {
        this.code = code;
        this.lastError = lastError;
    }

    @Nonnull
    public Code getCode() {
        return code;
    }

    @Nonnull
    public String getLastError() {
        return lastError;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() == Code.class) {
            return this.code == (Code) obj;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final ClientStatus other = (ClientStatus) obj;
        if (other.lastError == null || other.code == null) {
            return false;
        }
        return this.code == other.code && this.lastError.equals(other.lastError);
    }

    @Override
    public int hashCode() {
        return code.hashCode() + lastError.hashCode();
    }
}
