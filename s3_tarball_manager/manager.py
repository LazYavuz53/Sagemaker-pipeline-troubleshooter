import boto3
import tarfile
import os
import shutil
import logging
import argparse
 
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
 
class S3TarballManager:
    def __init__(self, s3_uri):
        self.s3_client = boto3.client('s3')
        self.bucket_name, self.key = self.parse_s3_uri(s3_uri)
        self.local_tarball = 'sourcedir.tar.gz'
        self.extract_dir = 'extracted_sourcedir'
        logging.debug(f"Using S3 bucket: {self.bucket_name}, key: {self.key}")
 
    def parse_s3_uri(self, s3_uri):
        if s3_uri.startswith("s3://"):
            s3_uri = s3_uri[5:]
        else:
            raise ValueError("Invalid S3 URI format. Must start with 's3://'.")
        bucket_name, key = s3_uri.split('/', 1)
        return bucket_name, key
 
    def download_tarball(self):
        logging.debug("Downloading tarball from S3...")
        self.s3_client.download_file(self.bucket_name, self.key, self.local_tarball)
        logging.debug(f"Downloaded tarball to {self.local_tarball}")
 
    def extract_tarball(self):
        logging.debug("Extracting tarball...")
        os.makedirs(self.extract_dir, exist_ok=True)
        with tarfile.open(self.local_tarball, 'r:gz') as tar:
            tar.extractall(path=self.extract_dir)
        logging.debug(f"Extracted tarball to {self.extract_dir}")
 
    def modify_script(self, script_name, modifications, line_insertions, line_deletions):
        script_path = os.path.join(self.extract_dir, script_name)
        logging.debug(f"Modifying script {script_path}...")
        with open(script_path, 'r') as file:
            script_content = file.readlines()
 
        logging.debug(f"Original script content: {script_content}")
 
        # Apply each text replacement modification
        for original_text, new_text in modifications.items():
            script_content = [line.replace(original_text, new_text) for line in script_content]
            logging.debug(f"Replaced '{original_text}' with '{new_text}'")
 
        # Apply each line insertion modification
        for line_number, new_line in sorted(line_insertions.items()):
            if 0 <= line_number < len(script_content):
                logging.debug(f"Modifying line {line_number}: {script_content[line_number]}")
                script_content[line_number] = new_line + '\n'
                logging.debug(f"New content for line {line_number}: {new_line}")
            else:
                logging.debug(f"Inserting line at position {line_number}: {new_line}")
                script_content.insert(line_number, new_line + '\n')
 
        # Apply each line deletion
        for line_number in sorted(line_deletions, reverse=True):
            if 0 <= line_number < len(script_content):
                logging.debug(f"Deleted line at position {line_number}: {script_content[line_number]}")
                del script_content[line_number]
 
        with open(script_path, 'w') as file:
            file.writelines(script_content)
        logging.debug(f"Modified script content: {script_content}")
 
    def package_tarball(self):
        logging.debug("Packaging tarball...")
        with tarfile.open(self.local_tarball, 'w:gz') as tar:
            for root, dirs, files in os.walk(self.extract_dir):
                for file in files:
                    tar.add(os.path.join(root, file), arcname=os.path.relpath(os.path.join(root, file), self.extract_dir))
        logging.debug(f"Packaged tarball to {self.local_tarball}")
 
    def upload_tarball(self):
        logging.debug("Uploading tarball to S3...")
        self.s3_client.upload_file(self.local_tarball, self.bucket_name, self.key)
        logging.debug(f"Uploaded tarball to s3://{self.bucket_name}/{self.key}")
 
    def cleanup(self):
        logging.debug("Cleaning up local files...")
        if os.path.exists(self.local_tarball):
            os.remove(self.local_tarball)
            logging.debug(f"Removed local tarball {self.local_tarball}")
        if os.path.exists(self.extract_dir):
            shutil.rmtree(self.extract_dir)
            logging.debug(f"Removed extracted directory {self.extract_dir}")
 
    def process_tarball(self, script_name, modifications, line_insertions, line_deletions):
        logging.debug("Starting tarball processing...")
        self.download_tarball()
        self.extract_tarball()
        self.modify_script(script_name, modifications, line_insertions, line_deletions)
        self.package_tarball()
        self.upload_tarball()
        self.cleanup()
        logging.debug("Finished tarball processing")
 
# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process a tarball from S3, modify a script, and upload it back to S3.')
    parser.add_argument('--s3-uri', required=True, help='The S3 URI for the tarball (e.g., s3://bucket/key)')
    parser.add_argument('--script', required=True, help='The script name to modify inside the tarball')
    parser.add_argument('--modifications', nargs='+', help='Text modifications in the format original_text=new_text', required=False)
    parser.add_argument('--insertions', nargs='+', help='Line insertions in the format line_number=new_line', required=False)
    parser.add_argument('--deletions', nargs='+', type=int, help='Line numbers to delete', required=False)
 
    args = parser.parse_args()
 
    modifications = {}
    if args.modifications:
        for mod in args.modifications:
            original_text, new_text = mod.split('=')
            modifications[original_text] = new_text
 
    line_insertions = {}
    if args.insertions:
        for ins in args.insertions:
            line_number, new_line = ins.split('=')
            line_insertions[int(line_number)] = new_line
 
    line_deletions = args.deletions if args.deletions else []
 
    manager = S3TarballManager(args.s3_uri)
    manager.process_tarball(args.script, modifications, line_insertions, line_deletions)