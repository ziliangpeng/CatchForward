import service
import unittest
import shutil
import time

class TestLevelDBStorage(unittest.TestCase):
    # TODO: mock time.time() and assert timestamp

    __TEST_DB_NAME = '__TEST_LEVEL_DB_DIR__'
    KEY1 = 'key1'
    KEY2 = 'key2'
    KEY3 = 'key3'
    CONTENT1 = 'content1'
    CONTENT2 = 'content2'
    CONTENT3 = 'content3'

    def setUp(self):
        self.storage = service.LevelDBStorage(self.__TEST_DB_NAME)

    def tearDown(self):
        del self.storage
        shutil.rmtree(self.__TEST_DB_NAME)

    def test_save(self):
        self.storage.save(self.KEY1, self.CONTENT1)

    def test_get_latest_content(self):
        self.storage.save(self.KEY1, self.CONTENT1)
        self.assertEqual(self.storage.get_latest_content(self.KEY1), self.CONTENT1)

    def test_get_content(self):
        self.storage.save(self.KEY1, self.CONTENT1)
        versions = self.storage.get_versions(self.KEY1)
        self.assertEqual(len(versions), 1)
        self.assertEqual(self.storage.get_content(self.KEY1, versions[0]), self.CONTENT1)

    def test_save_multiple(self):
        self.storage.save(self.KEY1, self.CONTENT1)
        time.sleep(0.01)
        self.storage.save(self.KEY1, self.CONTENT2)
        time.sleep(0.01)
        self.storage.save(self.KEY1, self.CONTENT3)
        time.sleep(0.01)

        versions = self.storage.get_versions(self.KEY1)
        self.assertEqual(len(versions), 3)

        self.assertEqual(self.storage.get_content(self.KEY1, versions[0]), self.CONTENT1)
        self.assertEqual(self.storage.get_content(self.KEY1, versions[1]), self.CONTENT2)
        self.assertEqual(self.storage.get_content(self.KEY1, versions[2]), self.CONTENT3)







if __name__ == '__main__':
    unittest.main()
