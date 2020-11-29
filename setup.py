import setuptools

setuptools.setup(
    name='nyc_taxi',
    version='0.0.1',
    description='Tools to analyze the nyc green taxi routes.',
    author='Marios Koulakis',
    classifiers=[
        'Development Status :gst: 3 - Alpha ',
        'Intended Audience :: Education',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License'
    ],
    keywords=[
        'taxi',
        'nyc',
        'time series'
    ],
    packages=['nyc_taxi'],
    install_requires=[
        'matplotlib',
        'numpy',
        'pandas',
        'tqdm',
    ],
    python_requires='>=3.6'
)

