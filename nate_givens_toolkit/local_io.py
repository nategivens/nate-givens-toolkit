import os

def get_file_extension(file):
    """Return the file extension of a file.
    
    :param file: File with a file extension to be split and returned
    """
    file_name, file_extension = os.path.splitext(file)
    return (file_extension.lower())

def file_exists_locally(local_dir, local_filename):
    """Return True if local_dir\local_filename exists, otherwise False
    
    :param local_dir: path to the local file
    :param local_filename: name of file to check
    """
    return os.path.isfile(join_dir_file(local_dir, local_filename))

def join_dir_file(local_dir, local_filename):
    """Return a string merging a directory and filename
    
    :param local_dir: path to the file
    :param local_filename: filename
    """
    return os.path.join(local_dir, local_filename)