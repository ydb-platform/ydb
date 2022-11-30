package ru.yandex.passport.tvmauth.roles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ConsumerRoles {
    private Map<String, ArrayList<HashMap<String, String>>> roles;

    ConsumerRoles(@Nonnull Map<String, ArrayList<HashMap<String, String>>> roles) {
        this.roles = roles;
    }

    @Nonnull
    public String debugPrint() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        return gson.toJson(this.roles);
    }

    public boolean hasRole(@Nonnull String rolename) {
        return this.roles.containsKey(rolename);
    }

    public ArrayList<HashMap<String, String>> getEntitiesForRole(@Nonnull String rolename) {
        return this.roles.get(rolename);
    }

    // TODO: PASSP-38421
    // public boolean checkRoleForExactEntity(@Nonnull String rolename, @Nonnull Map<String, String>[] exactEntity) {
    // }
}
