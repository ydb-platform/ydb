package ru.yandex.passport.tvmauth.roles;

import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.BlackboxEnv;
import ru.yandex.passport.tvmauth.CheckedServiceTicket;
import ru.yandex.passport.tvmauth.CheckedUserTicket;
import ru.yandex.passport.tvmauth.exception.NotAllowedException;

public class Roles {
    private Meta meta;
    private Map<Integer, ConsumerRoles> tvm;
    private Map<Long, ConsumerRoles> user;
    private String raw;

    public Roles(
        @Nonnull Meta meta,
        @Nonnull Map<Integer, ConsumerRoles> tvm,
        @Nonnull Map<Long, ConsumerRoles> user,
        @Nonnull String raw) {
        this.meta = meta;
        this.tvm = tvm;
        this.user = user;
        this.raw = raw;
    }

    @Nonnull
    public Meta getMeta() {
        return this.meta;
    }

    @Nonnull
    public String getRaw() {
        return this.raw;
    }

    public ConsumerRoles getRolesForService(@Nonnull CheckedServiceTicket checked) {
        if (!checked.booleanValue()) {
            throw new NotAllowedException("ServiceTicket is not valid");
        }

        return this.tvm.get(checked.getSrc());
    }

    public ConsumerRoles getRolesForUser(@Nonnull CheckedUserTicket checked) {
        commonUserChecks(checked);

        return this.user.get(checked.getDefaultUid());
    }

    public ConsumerRoles getRolesForUser(@Nonnull CheckedUserTicket checked, long selectedUid) {
        commonUserChecks(checked);
        if (!Arrays.stream(checked.getUids()).anyMatch(x -> x == selectedUid)) {
            throw new NotAllowedException("selectedUid must be in user ticket but it's not: " + selectedUid);
        }

        return this.user.get(selectedUid);
    }

    private void commonUserChecks(@Nonnull CheckedUserTicket checked) {
        if (!checked.booleanValue()) {
            throw new NotAllowedException("UserTicket is not valid");
        }
        if (checked.getEnv() != BlackboxEnv.PROD_YATEAM) {
            throw new NotAllowedException("User ticket must be from ProdYateam, got from " + checked.getEnv());
        }
    }

    // some shortcuts

    public boolean checkServiceRole(@Nonnull CheckedServiceTicket checked, @Nonnull String role) {
        ConsumerRoles c = getRolesForService(checked);
        if (c == null) {
            return false;
        }

        return c.hasRole(role);
    }

    public boolean checkUserRole(@Nonnull CheckedUserTicket checked, @Nonnull String role) {
        ConsumerRoles c = getRolesForUser(checked);
        if (c == null) {
            return false;
        }

        return c.hasRole(role);
    }

    public boolean checkUserRole(@Nonnull CheckedUserTicket checked, @Nonnull String role, long selectedUid) {
        ConsumerRoles c = getRolesForUser(checked, selectedUid);
        if (c == null) {
            return false;
        }

        return c.hasRole(role);
    }
}
