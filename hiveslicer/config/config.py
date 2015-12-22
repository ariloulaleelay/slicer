from pyhocon import ConfigFactory
from pyhocon.exceptions import ConfigException
import os

__all__ = [
    'ConfigException',
    'ConfigParserWithDefaults',
    'default_config',
]


class ConfigEntry(object):

    def __init__(self, config, key):
        self._config = config
        self._key = key

    def __str__(self):
        return self._config.get(self._key)

    def __call__(self, key=None):
        try:
            if key is None:
                return self._config.get(self._key)
            return ConfigEntry(self._config, self._key + '.' + key)
        except ConfigException, e:
            raise ConfigException(str(e))

    def __getattr__(self, key):
        try:
            if key.startswith('as_'):
                get_key = 'get_' + key[3:]
                return getattr(self._config, get_key)(self._key)
            return ConfigEntry(self._config, self._key + '.' + key)
        except ConfigException, e:
            raise ConfigException(str(e))


class ConfigParserWithDefaults(object):

    def __init__(self, default_path, config_path):
        default_config = ConfigFactory.parse_file(default_path)
        file_config = ConfigFactory.parse_file(config_path)
        self._config = file_config.with_fallback(default_config)

    def __getattr__(self, key):
        return ConfigEntry(self._config, key)

    def get_config(self, key):
        try:
            return self._config.get_config(key)
        except ConfigException, e:
            raise ConfigException(str(e))

    def get(self, key):
        try:
            return self._config.get(key)
        except ConfigException, e:
            raise ConfigException(str(e))


def default_config(path=None):
    default_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'default.conf')
    if path is None:
        path = default_path
    return ConfigParserWithDefaults(default_path, path)
