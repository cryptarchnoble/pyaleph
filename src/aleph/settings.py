from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class Settings:
    use_executors: bool


# Singleton
settings = Settings(
    use_executors=True,
)
