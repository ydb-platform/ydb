import library.python.testing.recipe
import library.recipes.docker_compose.lib as docker_compose_lib


if __name__ == "__main__":
    library.python.testing.recipe.declare_recipe(docker_compose_lib.start, docker_compose_lib.stop)
