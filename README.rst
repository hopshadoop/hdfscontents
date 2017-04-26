===========================================
HDFS Contents Manager for Jupyter Notebooks
===========================================

A contents manager for Jupyter that uses the Hadoop File System (HDFS) to store Notebooks and files


Getting Started
---------------

1. We assume you already have a running Hadoop Cluster and Jupyter

2. Set the JAVA_HOME and HADOOP_HOME environment variables

3. In some cases you also need to set the CLASSPATH

::

  export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

.. code: bash

4. Install HDFSContents Manager. This will also install dependencies such as Pydoop_

::

  pip install hdfscontents

.. code: bash

5. Configure and run Jupyter Notebook.

You can either use command line arguments to configure Jupyter to use the HDFSContentsManager class and set HDFS related configurations

::

  jupyter-notebook --NotebookApp.contents_manager_class='hdfscontents.hdfsmanager.HDFSContentsManager' \
        --HDFSContentsManager.hdfs_namenode_host='localhost' \
        --HDFSContentsManager.hdfs_namenode_port=9000 \
        --HDFSContentsManager.root_dir='/user/centos/'

.. code: bash

Alternatively, first run:

::
 
  jupyter-notebook --generate-config
 
.. code: bash
 
to generate a default config file. Edit and add the HDFS related configurations in the generated file. Then start the notebook server.
::
  jupyter-notebook


.. _Pydoop: http://crs4.github.io/pydoop/
