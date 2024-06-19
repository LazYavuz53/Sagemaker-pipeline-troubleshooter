from s3_tarball_manager.manager import S3TarballManager

bucket_name = 'your-bucket-name'
key = 'path/to/your/tarball.tar.gz'
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
