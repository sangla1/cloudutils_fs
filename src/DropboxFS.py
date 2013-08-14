import os
import six
from StringIO import StringIO

# python filesystem imports
from fs.base import FS
from fs.path import normpath
from fs.errors import PathError, UnsupportedError, \
                      CreateFailedError, ResourceInvalidError, \
                      ResourceNotFoundError, NoPathURLError
from fs.remote import RemoteFileBuffer
from fs.filelike import LimitBytesFile


#Dropbox specific imports
import dropbox


class DropboxFS(FS):
    """
        TODO: Description
    """
    
    _meta = { 'thread_safe' : True,
              'virtual': False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : True,
              'atomic.move' : True,
              'atomic.copy' : True,
              'atomic.makedir' : True,
              'atomic.rename' : False,
              'atomic.setconetns' : True
              }

    def __init__(self, root=None, credentials=None, thread_synchronize=True):
        self._root = root
        self._credentials = credentials
        
        if( self._credentials == None ):
            if( "DROPBOX_ACCESS_TOKEN" not in os.environ ):
                raise CreateFailedError("DROPBOX_ACCESS_TOKEN is not set in os.environ")
            else:
                self._credentials['access_token'] = os.environ.get('DROPBOX_ACCESS_TOKEN')
        
            
        super(DropboxFS, self).__init__(thread_synchronize=thread_synchronize)
    
    
    
    def __repr__(self):
        args = (self.__class__.__name__, self._root)
        
        return 'FileSystem: %s \nRoot: %s' % args

    __str__ = __repr__
    
    
    def _upload(self, path, data):
        if isinstance(data, basestring):
            string_data = data
        else:
            try:
                data.seek(0)
                string_data = data.read()
            except:
                raise ResourceInvalidError("Unsupported type")
            
        
        return self._cloud_command("put_file", path=path, data=string_data, overwrite=True)
    
    def setcontents(self, path, data="", encoding=None, errors=None, chunk_size=64*1024):
        """
        Sets contents to remote file.
        @param path: Full path to the file.
        @param data: File content as a string, or a StringIO object
        """ 
        if isinstance(data, six.text_type):
            data = data.encode(encoding=encoding, errors=errors)

        self._upload(path, data)
    
    def createfile(self, path, wipe=False):
        """Creates an empty file if it doesn't exist
        
        @param path: path to the file to create
        @param wipe: if True, existing file will be overwritten and it's contents will be erased
        @raise PathError: If the existing path is not valid. The reasons could be:
            provided path is the root path
            provided path is an existing file an wipe is set to False
        @raise UnsupportedError: When trying to create a file with the name of a existing directory
        """
        if( self.is_root(path) ):
            raise PathError(path)
        elif( self.exists(path) and not wipe ):
            raise PathError("File already exists")
        elif( self.exists(path) and self.isdir(path)):
            raise UnsupportedError("create a file with specified name. A folder with that name" + \
                                   " already exists")
        
        self._upload(path, "")
        
    def open(self, path, mode='r', buffering=-1, encoding=None, 
             errors=None, newline=None, line_buffering=False, **kwargs):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to cloud storage when the file is flushed or closed.
        """
        
        file_content = StringIO()
        
        if self.isdir(path):
            raise ResourceInvalidError("'%s' is a directory" % path)
        
        
        #  Truncate the file if requested
        if "w" in mode:
            self._upload(path, "")
        else:
            try:
                file_content.write( self._cloud_command("get_file", path=path ) )
                file_content.seek(0, 0)
            except:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError(path)
                else:
                    self._upload(path, "")
        
        f = LimitBytesFile(file_content.len, file_content, "r")
            
       
        #  For streaming reads, return the key object directly
        if mode == "r-":
            return f
        
        #  For everything else, use a RemoteFileBuffer.
        #  This will take care of closing the socket when it's done.
        return RemoteFileBuffer(self,path,mode,f)
   
        
    def is_root(self, path):
        if( path == self._root):
            return True
        else:
            return False
    def rename(self, src, dst, overwrite=False):
        if self.is_root(path = src) or self.is_root(path=dst):
            raise UnsupportedError("Can't rename the root directory")  
        
        resp = self._cloud_command('file_rename', from_path=src, to_path=dst)
        return resp
  
    def remove(self, path, checkFile = True):
        if self.is_root(path = path):
            raise UnsupportedError("Can't remove the root directory")   
             
        resp = self._cloud_command('file_delete', path=path)
        return resp
    
    def removedir(self, path):        
        if not self.isdir(path):
            raise PathError(path)
        if self.is_root(path = path):
            raise UnsupportedError("remove the root directory")
        
        return self.remove( path, False )
    
    def makedir(self, path, recursive=False, allow_recreate=False):
        #  
        #  @attention: dropbox currently doesn't support allow_recreate, so if a folder exists it will
        #      always throw an error. Independent of the allow_recrerate flag
        if self.is_root(path = path):
            raise UnsupportedError("recreate the root directory")
        if not self._checkRecursive(recursive, path):
            raise UnsupportedError("recursively create specified folder")
        if (not allow_recreate) and self.exists(path):
            raise UnsupportedError(" recreate specified folder")
        
        resp = self._cloud_command('create_folder', path=path, recursive=recursive, 
                                  allow_recreate=allow_recreate)
        
        return resp
    
    def _checkRecursive(self, recursive, path):
        #  Checks if the new folder to be created is compatible with current
        #  value of recursive
        parts = path.split("/")
        if( parts < 3 ):
            return True
        
        testPath = "/".join( parts[:-1] )
        if( self.exists(testPath) ):
            return True
        elif( recursive ):
            return True
        else:
            return False
    
    def isdir(self, path):
        try:
            info = self.getinfo(path)
        except:           
            raise PathError(path)
        
        return info['is_dir']
    
    def isfile(self, path):
        try:
            info = self.getinfo(path)
        except:           
            raise PathError(path)
        
        return not info['is_dir']

    
    def exists(self, path):
        try:
            self._cloud_command("metadata", path=path)
            return True
        except:
            return False
    
    def listdir(self, path="/",
                      wildcard=None,
                      full=False,
                      absolute=False,
                      dirs_only=False,
                      files_only=False,
                      overrideCache=False
                      ):
        
        data = self._cloud_command('metadata', path=path )
        flist = self._get_dir_list_from_service( data )

        dirContent = self._listdir_helper('/', flist, wildcard, full, absolute, dirs_only, files_only)
        return dirContent
    
    def _get_dir_list_from_service(self, metadata):
        flist = []
        if metadata and metadata.has_key('contents'):
            for one in metadata['contents']:
                flist.append(one['path'])
                
        return flist  
    
    def listdirinfo(self, path="./",
                          wildcard=None,
                          full=False,
                          absolute=False,
                          dirs_only=False,
                          files_only=False):
        
        metadata = self._cloud_command('metadata', path=path)
        
        path = normpath(path)
        def getinfo(p):
            if( metadata.has_key('contents') ):
                contents = metadata['contents']
                for one in contents:
                    if( one['path'] == p ):
                        return one
             
            return {}   

        return [(p, getinfo(p))
                    for p in self.listdir(path,
                                          wildcard=wildcard,
                                          full=full,
                                          absolute=absolute,
                                          dirs_only=dirs_only,
                                          files_only=files_only)]
    
        
  
        

            

    def getinfo(self, path):
        metadata = self._cloud_command('metadata', path=path)
        
        # Remove information about files in a directory
        if metadata.has_key('contents'):
            del metadata["contents"]
        
        return metadata
    
    def getpathurl(self, path, allow_none=False):
        """Returns a url that corresponds to the given path, if one exists.
        
        If the path does not have an equivalent URL form (and allow_none is False)
        then a :class:`~fs.errors.NoPathURLError` exception is thrown. Otherwise the URL will be
        returns as an unicode string.
        
        :param path: a path within the filesystem
        :param allow_none: if true, this method can return None if there is no
            URL form of the given path
        :type allow_none: bool
        :raises `fs.errors.NoPathURLError`: If no URL form exists, and allow_none is False (the default)
        :rtype: unicode 
        
        """
        
        url = None
        try:
            url = self._cloud_command("get_url", path=path)
        except:
            if not allow_none:
                raise NoPathURLError(path=path)

        return url
    
    
    def getDropBoxClient(self):
        access_token = self._credentials.get("access_token")
        try:
            return dropbox.client.DropboxClient(access_token)
        except:
            raise CreateFailedError("Access token is not valid")
        
    def _cloud_command(self, cmd, **kwargs):
        path = kwargs.get('path','/')
    
        client = self.getDropBoxClient()
            
        if cmd == 'metadata':
            # Return metadata for path
            resp = client.metadata( path )
            return resp
        elif cmd == 'get_url':
            # Returns file from dropbox service
            info = client.media(path)
            return info.get('url', None)
        elif cmd == 'create_folder':
            # Creates a new empty directory
            resp = client.file_create_folder( path )
            return resp
        elif cmd == 'file_delete':
            # Deletes file
            resp = client.file_delete( path )
            return resp
        elif cmd == 'file_move':
            from_path = kwargs.get('from_path','')
            to_path = kwargs.get('to_path','') 

            resp = client.file_move(from_path, to_path)
            return resp
        elif cmd == 'file_rename':
            from_path = kwargs.get('from_path','')
            to_path = kwargs.get('to_path','') 

            resp = client.file_move(from_path, to_path)
            return resp           
        elif cmd == 'put_file':
            # Puts file to dropbox
            overwrite = kwargs.get('overwrite', False)
            parent_rev = kwargs.get('parent_rev', None)
            resp = client.put_file(path, kwargs.get('data'), overwrite = overwrite, 
                                   parent_rev = parent_rev)
            return resp
            
        return None   
    
    
    
"""
Problems:
  - Flush and close, both call write contents and because of that 
    the file on cloud is overwrite twice...
"""