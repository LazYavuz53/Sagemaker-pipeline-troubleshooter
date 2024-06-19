
from setuptools import setup, find_packages

setup(
    name='s3-tarball-manager',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'boto3',
    ],
    author='Yavuz Kulaberoglu',
    author_email='yvauzkulaber53@hotmail.com',
    description='A Python library to manage tarballs in S3',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/LazYavuz53/s3-tarball-manager',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
