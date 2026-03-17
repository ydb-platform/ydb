Pure python scp module
======================

The scp.py module uses a paramiko transport to send and receive files via the
scp1 protocol. This is the protocol as referenced from the openssh scp program,
and has only been tested with this implementation.


Example
-------

..  code-block:: python

    from paramiko import SSHClient
    from scp import SCPClient

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('example.com')

    # SCPCLient takes a paramiko transport as an argument
    scp = SCPClient(ssh.get_transport())

    scp.put('test.txt', 'test2.txt')
    scp.get('test2.txt')

    # Uploading the 'test' directory with its content in the
    # '/home/user/dump' remote directory
    scp.put('test', recursive=True, remote_path='/home/user/dump')

    scp.close()


..  code-block::

    $ md5sum test.txt test2.txt
    fc264c65fb17b7db5237cf7ce1780769 test.txt
    fc264c65fb17b7db5237cf7ce1780769 test2.txt

Using 'with' keyword
--------------------

..  code-block:: python

    from paramiko import SSHClient
    from scp import SCPClient

    with SSHClient() as ssh:
        ssh.load_system_host_keys()
        ssh.connect('example.com')

        with SCPClient(ssh.get_transport()) as scp:
            scp.put('test.txt', 'test2.txt')
            scp.get('test2.txt')


..  code-block::

    $ md5sum test.txt test2.txt
    fc264c65fb17b7db5237cf7ce1780769 test.txt
    fc264c65fb17b7db5237cf7ce1780769 test2.txt


Uploading file-like objects
---------------------------

The ``putfo`` method can be used to upload file-like objects:

..  code-block:: python

    import io
    from paramiko import SSHClient
    from scp import SCPClient

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('example.com')

    # SCPCLient takes a paramiko transport as an argument
    scp = SCPClient(ssh.get_transport())

    # generate in-memory file-like object
    fl = io.BytesIO()
    fl.write(b'test')
    fl.seek(0)
    # upload it directly from memory
    scp.putfo(fl, '/tmp/test.txt')
    # close connection
    scp.close()
    # close file handler
    fl.close()


Tracking progress of your file uploads/downloads
------------------------------------------------

A ``progress`` function can be given as a callback to the SCPClient to handle
how the current SCP operation handles the progress of the transfers. In the
example below we print the percentage complete of the file transfer.

..  code-block:: python

    from paramiko import SSHClient
    from scp import SCPClient
    import sys

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('example.com')

    # Define progress callback that prints the current percentage completed for the file
    def progress(filename, size, sent):
        sys.stdout.write("%s's progress: %.2f%%   \r" % (filename, float(sent)/float(size)*100) )

    # SCPCLient takes a paramiko transport and progress callback as its arguments.
    scp = SCPClient(ssh.get_transport(), progress=progress)

    # you can also use progress4, which adds a 4th parameter to track IP and port
    # useful with multiple threads to track source
    def progress4(filename, size, sent, peername):
        sys.stdout.write("(%s:%s) %s's progress: %.2f%%   \r" % (peername[0], peername[1], filename, float(sent)/float(size)*100) )
    scp = SCPClient(ssh.get_transport(), progress4=progress4)

    scp.put('test.txt', '~/test.txt')
    # Should now be printing the current progress of your put function.

    scp.close()
