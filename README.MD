# S3 Tarball Manager

## Overview

The **S3 Tarball Manager** is a Python library designed to automate the process of downloading, modifying, and uploading tarballs stored in Amazon S3. This tool is particularly useful for scenarios where you need to programmatically update files within a compressed archive stored on S3 and then upload the updated archive back to S3.

## Features

- **Download Tarball**: Fetch a tarball from an S3 bucket using its URI.
- **Extract Tarball**: Unpack the tarball to a local directory.
- **Modify Script**: Make text replacements, insert lines, and delete lines in a specified script file within the tarball.
- **Repackage Tarball**: Compress the modified files back into a tarball.
- **Upload Tarball**: Upload the updated tarball back to the S3 bucket.
- **Cleanup**: Remove local files and directories used during the process to keep the environment clean.

## Installation

### Using Docker

Docker provides a consistent environment for running your application. To use Docker, follow these steps:

1. **Build the Docker Image**:

    ```sh
    docker build -t s3_tarball_manager .
    ```

2. **Run the Docker Container**:

    ```sh
    docker run --rm s3_tarball_manager
    ```

### Configuring AWS Account

To use this library, you need to configure your AWS account:

1. **Install AWS CLI**: Follow the instructions to install the AWS CLI from the [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

2. **Configure AWS CLI**: Run the following command and follow the prompts to enter your AWS access key, secret key, region, and output format.

    ```sh
    aws configure
    ```

    This will set up your credentials and default region in the `~/.aws/credentials` and `~/.aws/config` files.

## Usage

To use the library, create a Python script that utilizes the `S3TarballManager` class. Below is an example script:

### Example Usage

```python
from s3_tarball_manager import S3TarballManager

# Example usage
if __name__ == "__main__":
    s3_uri = 's3://absuite-gan-model/absuite-discovery-11-p-2ozaf1mrxyil/code/3a1591b597c6a6e14de0e574d827912b/sourcedir.tar.gz'
    script_name = 'preprocess.py'
    modifications = {
        '/opt/ml/processing/data/': '/opt/ml/processing/data/tensorflow'
    }
    line_insertions = {
        188: "# this section explains a feature engineering",
        122: "new content for line 122"
    }
    line_deletions = [
        200
    ]

    manager = S3TarballManager(s3_uri)
    manager.process_tarball(script_name, modifications, line_insertions, line_deletions)
