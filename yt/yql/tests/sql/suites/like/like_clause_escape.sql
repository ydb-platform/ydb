select key, subkey, value from plato.Input
where
value like "!%z" escape "!" or
value || "_" like "_?_" escape "?" or
value || "!" like "ddd!!" escape "!";