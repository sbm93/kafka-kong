import yaml
import os
from typing import Dict, Any

class Config:
    """
    Configuration management class.
    Loads configuration from a YAML file and provides access to configuration values.
    """
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize the Config class.

        :param config_path: Path to the configuration YAML file.
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as config_file:
            self._config = yaml.safe_load(config_file)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        :param key: The configuration key.
        :param default: Default value if the key is not found.
        :return: The configuration value.
        """
        return self._config.get(key, default)

    def get_nested(self, *keys: str, default: Any = None) -> Any:
        """
        Get a nested configuration value.

        :param keys: The nested keys to access the configuration value.
        :param default: Default value if the key is not found.
        :return: The configuration value.
        """
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
            else:
                return default
        return value

config = Config()