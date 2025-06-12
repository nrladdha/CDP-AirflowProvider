from setuptools import setup, find_packages

setup(
    name="cdp-airflow-provider",
    version="0.1.0",
    packages=find_packages(include=['cdp_provider*']),
    install_requires=[
        "apache-airflow>=2.5.0",
        "cdpcli>=0.1.0",
    ],
    description="Airflow provider for managing CDP clusters",
    url="https://github.com/nrladdha/CDP-AirflowProvider",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    entry_points={
        'apache_airflow_provider': [
            'provider_info=cdp_provider.__init__:get_provider_info',
        ],
    },
) 