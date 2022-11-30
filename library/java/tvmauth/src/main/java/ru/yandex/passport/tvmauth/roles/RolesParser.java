package ru.yandex.passport.tvmauth.roles;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RolesParser {
    private RolesParser() {
    }

    public static Roles parse(@Nonnull String raw) {
        JsonObject root = new JsonParser().parse(raw).getAsJsonObject();

        return new Roles(
            getMeta(root),
            getConsumers(root, "tvm", Integer.class),
            getConsumers(root, "user", Long.class),
            raw
        );
    }

    @Nonnull
    private static Meta getMeta(@Nonnull JsonObject root) {
        String revision = root.get("revision").getAsString();

        return new Meta(
            root.get("revision").getAsString(),
            new Date(root.get("born_date").getAsNumber().longValue() * 1000),
            new Date()
        );
    }

    private static <T> Map<T, ConsumerRoles> getConsumers(
        @Nonnull JsonObject node,
        @Nonnull String key,
        Class<T> type) {
        Map<T, ConsumerRoles> res = new HashMap<T, ConsumerRoles>();
        if (!node.has(key)) {
            return res;
        }

        JsonObject obj = node.getAsJsonObject(key);

        for (String id : obj.keySet()) {
            res.put(
                parseNum(id, type),
                getConsumerRoles(obj.getAsJsonObject(id))
            );
        }

        return res;
    }

    private static <T> T parseNum(String id, Class<T> type) {
        if (type == Integer.class) {
            return (T) Integer.valueOf(id);
        }
        return (T) Long.valueOf(id);
    }

    private static ConsumerRoles getConsumerRoles(@Nonnull JsonObject obj) {
        Map<String, ArrayList<HashMap<String, String>>> roles =
            new HashMap<String, ArrayList<HashMap<String, String>>>();

        for (String id : obj.keySet()) {
            roles.put(
                id,
                getEntities(obj.getAsJsonArray(id))
            );
        }

        return new ConsumerRoles(roles);
    }

    private static ArrayList<HashMap<String, String>> getEntities(@Nonnull JsonArray arr) {
        ArrayList<HashMap<String, String>> entities = new ArrayList<HashMap<String, String>>();

        for (int i = 0; i < arr.size(); i++) {
            entities.add(getEntity(arr.get(i).getAsJsonObject()));
        }

        return entities;
    }

    private static HashMap<String, String> getEntity(@Nonnull JsonObject obj) {
        HashMap<String, String> entity = new HashMap<String, String>();

        for (String key : obj.keySet()) {
            entity.put(
                key,
                obj.get(key).getAsString()
            );
        }

        return entity;
    }
}
