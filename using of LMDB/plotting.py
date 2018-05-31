import pickle
import lmdb
from matplotlib import pyplot as plt
from mpl_toolkits.basemap import Basemap
import progressbar
from settings import *


def compute_box(lmdb_location):
    """
    Computes the bounding box of a record.

    :param lmdb_location: the database to use
    """
    lmbd_env = lmdb.open(lmdb_location, readonly=False, create=False)

    bbox = [0, 100, -100, 0]

    with lmbd_env.begin() as txn:
        with txn.cursor() as curs:
            for key, data in curs:
                trip = pickle.loads(data)
                stLon = trip.start_station_longitude
                stLat = trip.start_station_latitude
                ndLon = trip.end_station_longitude
                ndLat = trip.end_station_latitude
                bbox[0], bbox[1] = min(bbox[0], stLon, ndLon), min(bbox[1], stLat, ndLat)
                bbox[2], bbox[3] = max(bbox[2], stLon, ndLon), max(bbox[3], stLat, ndLat)

    return bbox


def draw_map(centers, bbox, outof, lmdb_location):
    """
    Draw a map with trips and clusters

    :param centers: the clusters (K,p=4)
    :param bbox: the bounding box
    :param outof: the proportion of trips to draw
    :param lmdb_location: the database to use
    """
    K = centers.shape[0]
    fig, ax = plt.subplots()
    col = plt.cm.get_cmap('hsv', K)

    padding = .01
    lmbd_env = lmdb.open(lmdb_location, readonly=False, create=False)
    m = Basemap(projection='cyl', llcrnrlat=bbox[1] - padding, urcrnrlat=bbox[3] + padding,
                llcrnrlon=bbox[0] - padding, urcrnrlon=bbox[2] + padding, resolution='h')

    print("Getting NYC Map")
    m.arcgisimage(service='NatGeo_World_Map', verbose=True, xpixels=4000, interpolation='bicubic')

    print("Adding trips to the map")
    with lmbd_env.begin() as txn:
        with txn.cursor() as curs:
            progress_curs = progressbar.progressbar(curs, redirect_stdout=True)
            for i, key_n_data in enumerate(progress_curs):
                if i % outof == 0 and i < len(partition):
                    data = key_n_data[1]
                    trip = pickle.loads(data)
                    stLon = trip.start_station_longitude
                    stLat = trip.start_station_latitude
                    ndLon = trip.end_station_longitude
                    ndLat = trip.end_station_latitude
                    g = int(partition[i])
                    xst, yst = m(stLon, stLat)
                    xnd, ynd = m(ndLon, ndLat)
                    m.plot([xst, xnd], [yst, ynd], color=col(g), marker="+", alpha=.2, markeredgecolor=(0, 0, 0))

    for i, center in enumerate(centers):
        center_stLat, center_stLon, center_ndLat, center_ndLon = center
        xst, yst = m(center_stLon, center_stLat)
        xnd, ynd = m(center_ndLon, center_ndLat)
        m.plot([xst, xnd], [yst, ynd], color="black", alpha=.5, marker="o")

    plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
    plt.show()


if __name__ == '__main__':
    partition = pickle.load(open(dump_partition, "rb"))
    centers = pickle.load(open(dump_centers, "rb"))
    bbox = compute_box(lmdb_location)
    draw_map(centers, bbox, 20, lmdb_location)
