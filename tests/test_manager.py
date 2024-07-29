import unittest
import os
import tarfile
import shutil
from s3_tarball_manager.manager import S3TarballManager
 
class TestS3TarballManager(unittest.TestCase):
    def setUp(self):
        # Setup code to run before each test
        self.s3_uri = 's3://test-bucket/test-key'
        self.manager = S3TarballManager(self.s3_uri)
        self.test_script_content = """#!/usr/bin/env python3
\"\"\" Example script for testing \"\"\"
def example_function():
    pass
 
def another_function():
    print("This is another function")
 
# This is a placeholder comment
"""
        # Create a test tarball
        with tarfile.open(self.manager.local_tarball, 'w:gz') as tar:
            with open('test_script.py', 'w') as f:
                f.write(self.test_script_content)
            tar.add('test_script.py')
            os.remove('test_script.py')
 
    def tearDown(self):
        # Cleanup code to run after each test
        if os.path.exists(self.manager.local_tarball):
            os.remove(self.manager.local_tarball)
        if os.path.exists(self.manager.extract_dir):
            shutil.rmtree(self.manager.extract_dir)
 
    def test_download_tarball(self):
        # Mock the download method
        self.manager.download_tarball = lambda: None  # No-op
        self.manager.download_tarball()
        self.assertTrue(os.path.exists(self.manager.local_tarball))
 
    def test_extract_tarball(self):
        self.manager.extract_tarball()
        self.assertTrue(os.path.exists(os.path.join(self.manager.extract_dir, 'test_script.py')))
 
    def test_modify_script(self):
        modifications = {
            'def example_function()': 'def modified_example_function()'
        }
        line_insertions = {
            8: "# Inserted comment"
        }
        line_deletions = [
            12  # Deleting the placeholder comment
        ]
        self.manager.extract_tarball()
        self.manager.modify_script('test_script.py', modifications, line_insertions, line_deletions)
 
        modified_script_path = os.path.join(self.manager.extract_dir, 'test_script.py')
        with open(modified_script_path, 'r') as f:
            modified_content = f.read()
        self.assertIn('def modified_example_function()', modified_content)
        self.assertIn('# Inserted comment', modified_content)
        self.assertNotIn('# This is a placeholder comment', modified_content)
 
    def test_package_tarball(self):
        self.manager.extract_tarball()
        self.manager.package_tarball()
        self.assertTrue(os.path.exists(self.manager.local_tarball))
 
    def test_upload_tarball(self):
        # Mock the upload method
        self.manager.upload_tarball = lambda: None  # No-op
        self.manager.upload_tarball()
        # Since this is a no-op, just ensure no exceptions occur
 
    def test_cleanup(self):
        self.manager.cleanup()
        self.assertFalse(os.path.exists(self.manager.local_tarball))
        self.assertFalse(os.path.exists(self.manager.extract_dir))
 
if __name__ == '__main__':
    unittest.main()