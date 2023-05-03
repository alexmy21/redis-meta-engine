from pydantic import BaseSettings

class Settings(BaseSettings):
    # Get the environment variable or return the default value
    redis_host: str = "localhost"
    redis_port: int = 6379