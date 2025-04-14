from os import listdir
from pandas import read_table

class ingest_src():
    "Handles ingestion of delimited files from a source directory"

    def __init__(self,src_location:str,extension:str='.csv',compression:str=None,separator:str=';',classification:str=None):
        self.src_location =  str(src_location)
        self.extension = extension
        self.separator = separator
        self.compression = compression
        self.classification = classification
        return None

    def ingest(self) -> dict:
        """
        Primary function of this class.
        Executes the full ingestion process: finds files and loads them into a dict of dataframes

        Returns:
            dict: Dictionary of DataFrames (output of _create_df method)
        """
        self.src_file_list = self._get_src_files(self.src_location,self.extension,self.classification)
        self.df_dict = self._create_df(self.src_file_list,self.separator,self.compression)
        return self.df_dict

    def _get_src_files(self, loc:str, ext:str, prefix:str=None) -> list:
        """
        Retrieves specific extension files from the source directory with optional prefix

        Arg:
            loc (str): Directory path
            ext (str): File extension to match
            prefix (str, optional): Only include files starting with this prefix
        Returns:
            list: List of file paths
        """
        if not loc.endswith('\\'):
            loc += '\\'
        file_list = [loc+i for i in listdir(loc) if i.endswith(ext) and (not prefix or i.startswith(prefix))]
        return file_list

    def _create_df(self, file_list:list, sep:str, compression:str=None) -> dict:
        """
        Reads from a list of file and creates a dict of pd dataFrame with filename as keys

        Arg:
            file_list (list): List of file paths to load
            compression (str): Compression type used in the files
            sep (str): Delimiter used in the files
        Returns:
            dict: Dictionary of DataFrames keyed by filename
        """
        df_dict = {}
        for file in file_list:

            df_name = file.rsplit('\\',1)[-1].removesuffix(self.extension)
            df = read_table(file,sep=sep,compression=compression)
            df_dict[df_name] = df

            state = f'Loaded {df_name}'
            print(state)
            # log += state + '\n'
        return df_dict