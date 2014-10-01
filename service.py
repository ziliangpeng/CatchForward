import time


class NotInStorageError(Exception):
    pass


class Storage:

    def save(self, url, content):
        raise NotImplementedError()

    def get_versions(self, url):
        raise NotImplementedError()

    def exist_content(self, url):
        raise NotImplementedError()

    def get_content(self, url, timestamp):
        raise NotImplementedError()

    def get_latest_content(self, url):
        versions = self.get_versions(url)
        if len(versions) == 0:
            raise NotInStorageError()

        latest_version = max(versions)
        return self.get_content(url, latest_version)


class LevelDBStorage(Storage):

    __DB_DIR = '_CatchForward_LevelDB_dir'
    __KEY_SEPARATOR = '  __|||__  '
    __VERSION_SUFFIX = 'VERSIONS'

    def __init__(self, db_name=None):
        import leveldb
        import json
        if db_name == None:
            db_name = self.__DB_DIR
        self.db = leveldb.LevelDB(db_name)
        self.jsonlib=json

    @classmethod
    def _make_url_key(cls, url, timestamp):
        return url + cls.__KEY_SEPARATOR + str(timestamp)

    @classmethod
    def _make_versions_key(cls, url):
        return url + cls.__KEY_SEPARATOR + cls.__VERSION_SUFFIX

    def _save_versions(self, url, versions):
        version_key = self._make_versions_key(url)
        versions_json = self.jsonlib.dumps(versions)
        self.db.Put(version_key, versions_json)

    def get_versions(self, url):
        version_key = self._make_versions_key(url)
        try:
            versions_json = self.db.Get(version_key)
        except KeyError:
            raise NotInStorageError()
        return self.jsonlib.loads(versions_json)

    def save(self, url, content):
        timestamp = int(1000 * time.time()) # in ms
        url_key = self._make_url_key(url, timestamp)
        self.db.Put(url_key, content)

        try:
            versions = self.get_versions(url)
        except NotInStorageError:
            versions = []
        versions.append(timestamp)
        self._save_versions(url, versions)

    def exist_content(self, url):
        versions_key = self._make_versions_key(url)
        try:
            self.db.Get(versions_key)
            return True
        except KeyError:
            return False

    def get_content(self, url, timestamp):
        url_key = self._make_url_key(url, timestamp)
        
        try:
            content = self.db.Get(url_key)
        except KeyError:
            raise NotInStorageError()

        return content

