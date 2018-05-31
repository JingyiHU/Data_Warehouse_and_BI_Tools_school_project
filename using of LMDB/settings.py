# Various Settings for the projects
import numpy as np

# https://www.citibikenyc.com/system-data
filename = "./data/201803_citibikenyc_tripdata.csv"


# A description of the fields presents in the dataset
class Record:
    """
    Class representing a record of the file
    """

    fields = [("tripduration", int),
              ("starttime", str),
              ("stoptime", str),
              ("start station id", int),
              ("start station name", str),
              ("start station latitude", float),
              ("start station longitude", float),
              ("end station id", int),
              ("end station name", str),
              ("end station latitude", float),
              ("end station longitude", float),
              ("bikeid", int),
              ("name_localizedValue0", str),
              ("usertype", str),
              ("birth year", str),
              ("gender", str)]

    def __init__(self, infos):
        # Ugly mapping that has to be done

        # Not storing everything to be faster (reducing the deserialisation time)
        self.start_station_latitude = infos[5]
        self.start_station_longitude = infos[6]
        self.end_station_latitude = infos[9]
        self.end_station_longitude = infos[10]

    def get_4D_representation(self):
        return np.array([self.start_station_latitude,
                         self.start_station_longitude,
                         self.end_station_latitude,
                         self.end_station_longitude])


# The location of the lmdb database
lmdb_location = "database"
lmdb_location_test = "test"

# Dumps
dump_partition = "dump/partition.pickle"
dump_centers = "dump/centers.pickle"

dump_partition_test = dump_partition + ".test"
dump_centers_test = dump_centers + ".test"

# The maximum size of the database (defaultValue: 10485760) (see
# https://lmdb.readthedocs.io/en/release/#transaction-management)
map_size = 1000000000


class KmeanResults:

    def __init__(self, k, partition, centers, n_iter, totalwithnss):
        self.k = k
        self.partition = partition
        self.centers = centers
        self.n_iter = n_iter
        self.totalwithnss = totalwithnss

    def __str__(self):
        string = "Kmeans results (k={})\n  - n_iter = {}\n  - totalwithnss = {}".format(self.k, self.n_iter, self.totalwithnss)
        return string