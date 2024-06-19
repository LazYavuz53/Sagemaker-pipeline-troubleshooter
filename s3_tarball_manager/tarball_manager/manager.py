import boto3
import tarfile
import os
import shutil

class S3TarballManager:
    def __init__(self, bucket_name, key):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.key = key
        self.local_tarball = 'sourcedir.tar.gz'
        self.extract_dir = 'extracted_sourcedir'

    def download_tarball(self):
        print("Downloading tarball from S3...")
        self.s3_client.download_file(self.bucket_name, self.key, self.local_tarball)

    def extract_tarball(self):
        print("Extracting tarball...")
        os.makedirs(self.extract_dir, exist_ok=True)
        with tarfile.open(self.local_tarball, 'r:gz') as tar:
            tar.extractall(path=self.extract_dir)

    def modify_script(self, script_name, modifications, line_insertions, line_deletions):
        script_path = os.path.join(self.extract_dir, script_name)
        print(f"Modifying script {script_path}...")
        with open(script_path, 'r') as file:
            script_content = file.readlines()

        # Apply each text replacement modification
        for original_text, new_text in modifications.items():
            script_content = [line.replace(original_text, new_text) for line in script_content]

        # Apply each line insertion modification
        for line_number, new_line in sorted(line_insertions.items()):
            script_content.insert(line_number, new_line + '\n')

        # Apply each line deletion
        for line_number in sorted(line_deletions, reverse=True):
            if 0 <= line_number < len(script_content):
                del script_content[line_number]

        with open(script_path, 'w') as file:
            file.writelines(script_content)

    def package_tarball(self):
        print("Packaging tarball...")
        with tarfile.open(self.local_tarball, 'w:gz') as tar:
            for root, dirs, files in os.walk(self.extract_dir):
                for file in files:
                    tar.add(os.path.join(root, file), arcname=os.path.relpath(os.path.join(root, file), self.extract_dir))

    def upload_tarball(self):
        print("Uploading tarball to S3...")
        self.s3_client.upload_file(self.local_tarball, self.bucket_name, self.key)

    def cleanup(self):
        print("Cleaning up local files...")
        if os.path.exists(self.local_tarball):
            os.remove(self.local_tarball)
        if os.path.exists(self.extract_dir):
            shutil.rmtree(self.extract_dir)

    def process_tarball(self, script_name, modifications, line_insertions, line_deletions):
        self.download_tarball()
        self.extract_tarball()
        self.modify_script(script_name, modifications, line_insertions, line_deletions)
        self.package_tarball()
        self.upload_tarball()
        self.cleanup()

# Example usage
if __name__ == "__main__":
    bucket_name = 'sagemaker-eu-west-1-467990857322'
    key = 'absuite-discovery-11-p-2ozaf1mrxyil/code/60341dc08b9c635388943b06c8fd77dc/sourcedir.tar.gz'
    script_name = 'preprocess.py'
    modifications = {
        '/opt/ml/processing/data/': '/opt/ml/processing/data/tensorflow'
    }
    line_insertions = {
        188: "# this section explains a feature engineering"
    }
    line_deletions = [
        200  # Assuming you want to delete line 200
    ]

    manager = S3TarballManager(bucket_name, key)
    manager.process_tarball(script_name, modifications, line_insertions, line_deletions)
