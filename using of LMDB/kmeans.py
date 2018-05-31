import lmdb
import pickle
import progressbar


from settings import *


def get_4D_representation(lmdb_env, key_str):
    """
    From a given key, return a 4D representation of the record (coordinates of the stations)
    :param lmdb_env:
    :param key_str: the key
    :return: a (4,) numpy array of float
    """

    with lmdb_env.begin(write=True) as transaction:
        blob = transaction.get(key_str)
        record = pickle.loads(blob)
        repr = record.get_4D_representation()
        return repr


def compute_distance(point_repr, clusters):
    """
    Compute the squared distance between one point and some clusters
    :param point_repr: a point (4,)
    :param clusters: some clusters (k,b)
    :return: a array of size (k,) giving the squared distance.
    """
    list_dist = [np.sum(np.power(point_repr - cluster, 2)) for cluster in clusters]
    distances = np.array(list_dist)

    return distances


def kmeans_ess(K, lmdb_env, n_iter=100, eps=10e-7, verbose=False):
    """
    One trial of kmeans.

    :param K: the number of clusters to use
    :param lmdb_env: the environment of the database to use
    :param n_iter: the maximum number of iterations
    :return: a KmeansResults containing info about the classification.
    """
    def log_p(s):
        if verbose:
            print(s)

    # Parameters
    db_stats = lmdb_env.stat()
    n = db_stats["entries"]
    p = 4
    log_p("Data : ({},{})".format(n, p))

    clusters = np.zeros((K, p))

    # Initialisation des centroïdes
    clusters_keys = np.random.randint(low=2, high=n, size=K)
    for i, key in enumerate(clusters_keys):
        clusters[i, :] = get_4D_representation(lmdb_env, str(key).encode('utf-8'))
    old_clusters = np.copy(clusters)

    aggregated_points_for_clusters = np.zeros((K, p))
    aggregated_dist_for_clusters = np.zeros((K, 1))
    nb_points_for_clusters = np.zeros((K, 1))
    partition = np.zeros((n,))
    compt_iter = 0
    while compt_iter < n_iter:
        log_p("Iteration {}".format(n_iter))
        # un seul passage sur tous les points :
        with lmdb_env.begin() as txn:
            with txn.cursor() as curs:
                i_point = -1
                for key_str, value in curs:
                    i_point += 1
                    point_repr = pickle.loads(value).get_4D_representation()
                    dist_to_clusters = compute_distance(point_repr, clusters)
                    min_dist = np.min(dist_to_clusters)
                    k = np.argmin(dist_to_clusters)

                    aggregated_dist_for_clusters[k] = min_dist
                    aggregated_points_for_clusters[k] += point_repr
                    nb_points_for_clusters[k] += 1
                    partition[i_point] = k

        nb_points = np.repeat(nb_points_for_clusters, repeats=4, axis=1)

        clusters = aggregated_points_for_clusters / nb_points

        # Critère de convergence
        deplacement = np.sum(np.power(old_clusters - clusters, 2))
        if deplacement < eps:
            log_p("Convergence reached")
            break

        old_clusters = np.copy(clusters)
        compt_iter += 1

    totalwithnss = np.sum(aggregated_dist_for_clusters)

    res = KmeanResults(k=K, partition=partition, centers=clusters, n_iter=compt_iter, totalwithnss=totalwithnss)
    return res


def kmeans(K, n_ess, lmdb_location, verbose=False):
    """
    Returns a KmeansResults containing info about the classification
    of the dataset.

    :param K: the number of clusters to use
    :param n_ess: the number of trials to perform
    :param lmdb_location: the location of the database
    :return: a KmeansResults containing info about the classification.
    """
    # Getting the database, throws error if not present
    lmdb_env = lmdb.open(lmdb_location, readonly=False, create=False)

    best_res = None
    best_withnss = np.finfo(np.float).max

    # Doing n_ess trials
    for i in progressbar.progressbar(range(n_ess), redirect_stdout=True):
        res = kmeans_ess(K, lmdb_env, verbose=verbose)

        # Selecting the best one
        if res.totalwithnss < best_withnss:
            best_withnss = res.totalwithnss
            best_res = res

    return best_res


if __name__ == '__main__':
    n_ess = 10
    k = 5
    res = kmeans(k, n_ess, lmdb_location_test, verbose=False)

    centers = res.centers
    partition = res.partition
    print(res)
    print(res.centers)

    pickle.dump(centers, open(dump_centers_test, "wb"))
    pickle.dump(partition, open(dump_partition_test, "wb"))

