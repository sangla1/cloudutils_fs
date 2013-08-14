import os
import six
from StringIO import StringIO
import mimetypes

# python filesystem imports
from fs.base import FS
from fs.path import normpath
from fs.errors import PathError, UnsupportedError, \
                      CreateFailedError, ResourceInvalidError, \
                      ResourceNotFoundError, NoPathURLError
from fs.remote import RemoteFileBuffer
from fs.filelike import LimitBytesFile

# Imports specific to google drive service
import datetime
import httplib2
from apiclient.discovery import build
from apiclient import errors
from apiclient.http import MediaFileUpload, MediaInMemoryUpload

#Dropbox specific imports
import dropbox
from gi.overrides import override


class GoogleDriveFS(FS):
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
        self.cached_files = {}
        
        if (self._root == None):
            service = self._build_service(self._credentials)
            about = service.about().get().execute()
            self._root = about.get("rootFolderId")
        
        """
        if( self._credentials == None ):
            if( "DROPBOX_ACCESS_TOKEN" not in os.environ ):
                raise CreateFailedError("DROPBOX_ACCESS_TOKEN is not set in os.environ")
            else:
                self._credentials['access_token'] = os.environ.get('DROPBOX_ACCESS_TOKEN')
        """
            
        super(GoogleDriveFS, self).__init__(thread_synchronize=thread_synchronize)

        
    
    def __repr__(self):
        args = (self.__class__.__name__, self._root)
        
        return 'FileSystem: %s \nRoot: %s' % args

    __str__ = __repr__
    
    
    def _update(self, file_id, data):
        if isinstance(data, basestring):
            string_data = data
        else:
            try:
                data.seek(0)
                string_data = data.read()
            except:
                raise ResourceInvalidError("Unsupported type")
            
        
        return self._cloud_command("update_file", file_id=file_id, content=string_data)
    
    def setcontents(self, file_id, data="", encoding=None, errors=None, chunk_size=64*1024):
        """
        Sets contents to remote file.
        @param path: Full path to the file.
        @param data: File content as a string, or a StringIO object
        """ 
        if isinstance(data, six.text_type):
            data = data.encode(encoding=encoding, errors=errors)

        self._update(file_id, data)

    
    def createfile(self, title, parent_id=None, description=""):
        """Creates an empty file always, even if another file with the same name exists
        
        @param title: title of the new file, with extension
        @param parent: folder in which the file will be inserted, default: root
        @raise PathError: If parent doesn't exist
        """
        if( parent_id==None ):
            parent_id = self._root
        elif( not self.exists(parent_id) ):
            raise PathError("parent doesn't exist")
        
        self._cloud_command("create_new_file", title=title, parent_id=parent_id, description=description)
        
    def open(self, file_id, mode='r',  buffering=-1, encoding=None, 
             errors=None, newline=None, line_buffering=False, **kwargs):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to cloud storage when the file is flushed or closed.
        """
        
        file_content = StringIO()
        
        if self.isdir(file_id):
            raise ResourceInvalidError("'%s' is a directory" % file_id)
        
        
        #  Truncate the file if requested
        if "w" in mode:
            self._update(file_id, "")
        else:
            try:
                file_content.write( self._cloud_command("get_file", file_id=file_id ) )
                file_content.seek(0, 0)
            except Exception, e:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError("%r" % e)
                else:
                    self._upload(file_id, "")
        
        f = LimitBytesFile(file_content.len, file_content, "r")
            
       
        #  For streaming reads, return the key object directly
        if mode == "r-":
            return f
        
        #  For everything else, use a RemoteFileBuffer.
        #  This will take care of closing the socket when it's done.
        return RemoteFileBuffer(self,file_id,mode,f)
   
        
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
    
    def makedir(self, title, file_id, recursive=False ):        
        return self._cloud_command("file_create_folder", title=title)
        
    
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
    
    def isdir(self, file_id):
        try:
            info = self.getinfo(file_id)
        except:           
            raise PathError(file_id)
        
        return info["mimeType"] == "application/vnd.google-apps.folder"
    
    def isfile(self, file_id):
        try:
            info = self.getinfo(file_id)
        except:           
            raise PathError(file_id)
        
        return info["mimeType"] != "application/vnd.google-apps.folder"

    
    def exists(self, file_id):
        try:
            self.getinfo(file_id)
            return True
        except:
            return False
    
    
    def _get_dir_list_from_service(self, metadata):
        flist = []
        if metadata and metadata.has_key('items'):
            for one in metadata['items']:
                flist.append(one['id'])
                
        return flist 
    
    def listdir(self, path=None,
                      wildcard=None,
                      full=False,
                      absolute=False,
                      dirs_only=False,
                      files_only=False,
                      overrideCache=False
                      ):
        if( not path ):
            path = self._root
        
        data = self.cloud_command('list_dir', path=path )
        flist = self._get_dir_list_from_service( data )

        dirContent = self._listdir_helper('/', flist, wildcard, full, absolute, dirs_only, files_only)
        return dirContent
    
    #Optimised listdir from pyfs
    def listdirinfo(self, path=None,
                          wildcard=None,
                          full=False,
                          absolute=False,
                          dirs_only=False,
                          files_only=False):
        
        if( not path ):
            path = self._root
        
        metadata = self.cloud_command('list_dir', path=path)
        
        def getinfo(p):
            if( metadata.has_key('items') ):
                contents = metadata['items']
                for one in contents:
                    if( one['id'] == p ):
                        return one
             
            return {}   

        return [(p, getinfo(p))
                    for p in self.listdir(path,
                                          wildcard=wildcard,
                                          full=full,
                                          absolute=absolute,
                                          dirs_only=dirs_only,
                                          files_only=files_only)]

        
        

            

    def getinfo(self, file_id):
        resp = self._cloud_command("get_file_info", file_id = file_id)
        return resp
        
    
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
    
    
    def _build_service(self, credentials):
        http = httplib2.Http()
        http = credentials.authorize(http);
        service = build('drive', 'v2', http=http)
        return service
        
    def _cloud_command(self, cmd, **kwargs):
        file_id = kwargs.get('file_id', self._root)
        service = self._build_service(self._credentials)
        
        if cmd == 'list_dir':
            # Return directory list
            resp = service.files().list().execute()
            return resp
        elif cmd == 'get_file':
            if( not self.cached_files.has_key(file_id) ): 
                f = service.files().get(fileId=file_id).execute()
                self.cached_files[f["id"]] = f
            else:
                f = self.cached_files[file_id]
                
            
            download_url = f.get('downloadUrl')
            resp, content = service._http.request(download_url)
            if( resp.status == 200 ):
                return content
            else:
                return None   
        elif cmd == 'get_file_info':
            f = service.files().get(fileId=file_id).execute()
            return f  
        elif cmd == 'create_new_file':
            title = kwargs.get("title", "untitled.txt")
            parent_id = kwargs.get("parent_id", self._root)
            description = kwargs.get("description", "")
            body = {
                    "title": title,
                    "parents": [{"id": parent_id}],
                    "description": description,
                    "mimeType": mimetypes.guess_type(title)
                    }
            f = service.files().insert(
                                       body = body
                                       ).execute()

            return f 
        elif cmd == 'file_create_folder':
            # Creates a new empty directory
            parent_id = kwargs.get("parent_id", self._root)
            title = kwargs.get("title", "untitled")
            body = {
                    "title": title,
                    "parents": [{"id": parent_id}],
                    "mimeType": "application/vnd.google-apps.folder"
                    }
            resp = service.files().insert(body=body).execute()
            return resp
        elif cmd == 'file_delete':
            # Deletes file
            resp = service.files().delete(fileId=file_id).execute()
            # Return empty body if everything was OK
            return resp
        elif cmd == 'file_move':
            from_path = kwargs.get('from_path','')
            to_path = kwargs.get('to_path','') 
        
            resp = self.client.file_move(from_path, to_path)
            return resp        
        elif cmd == 'update_file':
            # Updates a file on google drive
            f = self.cached_files.get(file_id)
            content = kwargs.get("content")
            media_body = MediaInMemoryUpload(content)
            updated_file = service.files().update(
                                                  fileId = file_id,
                                                  body = f,
                                                  media_body=media_body).execute()
            self.cached_files[file_id] = updated_file
        return None
    
    
    
"""
Problems:
  - Flush and close, both call write contents and because of that 
    the file on cloud is overwrite twice...
"""