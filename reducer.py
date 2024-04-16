import os
import grpc
from concurrent import futures
import numpy as np
import logging

import kmeans_pb2
import kmeans_pb2_grpc

class Reducer(kmeans_pb2_grpc.KMeansClusterServicer):
    def shuffle_and_sort(self, intermediate_values):
        sorted_intermediate_values = sorted(intermediate_values, key=lambda x: x[0])
        grouped_values = {}
        for key, value in sorted_intermediate_values:
            if key not in grouped_values:
                grouped_values[key] = []
            grouped_values[key].append(value)
        return grouped_values

    def reduce_function(self, centroid_id, grouped_values):
        points = grouped_values[centroid_id]
        new_centroid = [sum(x) / len(points) for x in zip(*points)]
        return centroid_id, new_centroid

    def ProcessDataForReducer(self, request, context):
        intermediate_values = []
        # Simulated retrieval of intermediate values from mappers via gRPC
        for i in range(request.num_mappers):
            mapper_dir = f'mapper_{i}'
            with open(os.path.join(mapper_dir, f'partition_{request.reducer_id}.txt'), 'r') as file:
                lines = file.readlines()
                for line in lines:
                    centroid, point = line.split(maxsplit=1)
                    intermediate_values.append((eval(centroid), eval(point)))

        grouped_values = self.shuffle_and_sort(intermediate_values)
        new_centroids = [self.reduce_function(request.reducer_id, grouped_values)]

        # Store new centroid results to output file
        output_dir = f'reducer_{request.reducer_id}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, 'output.txt')
        with open(output_path, 'w') as f:
            for centroid_id, new_centroid in new_centroids:
                f.write(f"{centroid_id} {new_centroid}\n")

        return kmeans_pb2.ReducerResponse(reducer_id=request.reducer_id, new_centroids=new_centroids, status="SUCCESS")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kmeans_pb2_grpc.add_KMeansClusterServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    server.wait_for termination()

if __name__ == '__main__':
    logging.basicConfig(filename='reducer_log.txt', level=logging.INFO)
    serve()
