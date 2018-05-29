from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def withColumnsRenamed(self, rename_dict):
    '''
    Accepts a dictionary of {'old_name': 'new_name'}. Returns a Dataframe
    identical to self, except with the specified renaming applied.
    '''
    return reduce(
        lambda df, c: df.withColumnRenamed(c, rename_dict[c]),
        rename_dict.keys(),
        self)

