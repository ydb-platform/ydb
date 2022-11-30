package ru.yandex.passport.tvmauth.roles;

import java.util.Date;

import javax.annotation.Nonnull;

public class Meta {
    private String revision;
    private Date bornTime;
    private Date appliedTime;

    public Meta(@Nonnull String revision, @Nonnull Date bornTime, @Nonnull Date appliedTime) {
        this.revision = revision;
        this.bornTime = bornTime;
        this.appliedTime = appliedTime;
    }

    @Nonnull
    public String getRevision() {
        return this.revision;
    }

    @Nonnull
    public Date getBornTime() {
        return this.bornTime;
    }

    @Nonnull
    public Date getAppliedTime() {
        return this.appliedTime;
    }
}
