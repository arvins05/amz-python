from setuptools import setup, find_packages

# Reading requirements from requirements.txt
def read_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()

setup(
    name='common_imports',
    version='0.1',
    packages=find_packages(),
    install_requires=read_requirements(),  # Automatically install from requirements.txt
)
