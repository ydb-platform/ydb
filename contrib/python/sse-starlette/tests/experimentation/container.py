from time import sleep

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy


# Define a simple container
class BasicContainer(DockerContainer):
    def __init__(self):
        # super().__init__("python:3.12-slim")
        super().__init__("sse_starlette:latest")

        self.app_path = "tests.integration.main_endless:app"

        self.with_volume_mapping(
            host="/Users/Q187392/dev/s/public/sse-starlette", container="/app"
        )
        self.with_name("sse_starlette")
        self.with_exposed_ports(8000)

        # Set a basic command to keep the container running
        # self.with_command("tail -f /dev/null")
        self.with_command(
            f"uvicorn {self.app_path} --host 0.0.0.0 --port 8000 --log-level debug"
        )

        # Wait for server to be ready (applied during start())
        self.waiting_for(LogMessageWaitStrategy("Application startup complete"))


if __name__ == "__main__":
    # Start the container
    container = BasicContainer()
    with container:
        print(f"Container is running. ID: {container._container.id}")
        print(
            f"Exec into the container using: docker exec -it {container._container.id} sh"
        )

        print(f"http://localhost:{container.get_exposed_port(8000)}/endless")
        sleep(100000)  # Keep the container running
