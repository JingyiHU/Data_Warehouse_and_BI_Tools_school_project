import lmdb
import progressbar
import pickle
import csv

from settings import *


def extract_info(fields_data, fields_description):
    """
    Extract the info contained in a string using descriptions passed as arguments.

    :param fields_data: a list of string
    :param fields_description: List of (fieldName, type of the field)
    :return: a list of variables
    """
    infos = []

    for i, data in enumerate(fields_data):
        type = fields_description[i][1]
        infos.append(type(data))

    nb_missing_fields = len(fields_description) - len(infos)
    if nb_missing_fields != 0:
        print("{} missing fields : Appending None".format(nb_missing_fields))
        for _ in range(nb_missing_fields):
            infos.append(None)

    record = Record(infos)

    return record


def populate_database(file_name, fields, lmdb_location, map_size, limit=np.finfo(float).max):
    """
    Populate a LBDM database using a file and a description of the data.


    :param file_name: the file containing the data
    :param fields: list of (fieldName, type of the field)
    :param lmdb_location: the location of the database
    :param map_size: the maximum size of the database
    :param limit: number of records to store

    """

    # Getting the database, creating it if missing
    database_env = lmdb.open(lmdb_location, readonly=False, map_size=map_size)

    # Opening the file in text mode
    with open(file_name, 'rt', encoding="ascii") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')

        with database_env.begin(write=True) as transaction:
            key = -2

            # Custom progress bar
            rows = progressbar.progressbar(csv_reader, redirect_stdout=True)

            for row in rows:
                key += 1
                if key > limit:
                    break

                # We do not store the header
                if key == -1:
                    continue

                # Extracting the info from one line and serializing it
                data = extract_info(row, fields)
                byte_array = pickle.dumps(data)

                # Inserting in the database
                transaction.put(str(key).encode('utf-8'), byte_array)


if __name__ == '__main__':
    populate_database(filename, Record.fields, lmdb_location_test, map_size,limit=200)
