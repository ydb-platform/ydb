class ModelFactory:
    model_name = None

    def __call__(self, manager):
        """
        Create model class but only if it doesn't already exist
        in declarative model registry.
        """
        Base = manager.declarative_base
        registry = Base.registry._class_registry
        if self.model_name not in registry:
            return self.create_class(manager)
        return registry[self.model_name]
