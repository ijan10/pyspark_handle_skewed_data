from distutils.core import setup
setup(
  name = 'join_skew_data',         # How you named your package folder (MyLib)
  packages = ['join_skew_data'],   # Chose the same as "name"
  version = '0.1',      # Start with a small number and increase it with every change you make
  license='Undertone',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'customize partitions with skew data for join',   # Give a short description about your library
  author = 'itzik jan',                   # Type in your name
  author_email = 'itzikjan@gmail.com',      # Type in your E-Mail
  url = 'https://github.com/ijan10/pyspark_handle_skewed_data',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/ijan10/pyspark_handle_skewed_data/archive/v_0.1.tar.gz',    # I explain this later on
  keywords = ['PySpark', 'skew', 'join'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'pyspark.sql',
          'operator'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 2.7'
  ],
)