import math
import random
import argparse
from dataclasses import dataclass, field

# I made this primarily for convenience. It makes writing some of the update logic
# MUCH easier. 
@dataclass
class Node:
    id: int
    partition: int = -1
    hyperedges: set = field(default_factory=set)
    connected_nodes: set = field(default_factory=set)
    gain = 0
    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, Node) and self.id == other.id



class FMPartition:
    def __init__(self, filename):
        random.seed(42)
        self.nodes = {}  
        self.hyperedges = {}
        self.buckets = [{}, {}]
        self.locked_nodes = set()
        self.max_cardinality = 0
        edge_idx = 1
        first_line = True
        PACE_HGR = False
        self.max_iterations = 500

        try:
            f = open(filename, 'r')
        except FileNotFoundError:
            print(f'{filename} does not exist.')

        for line in f:
            if 'c' in line:
                continue
            args = line.split()
            if 'p' in line:
                self.vertices = int(args[2])
                self.edges = int(args[3])
                first_line = False
                PACE_HGR = True
                continue
            if first_line:
                self.vertices = int(args[1])
                self.edges = int(args[0])
                first_line = False
                continue

            if PACE_HGR:
                edge_nodes = args[1:]
                self.hyperedges[args[0]] = edge_nodes
                edge_idx = args[0]
            else:
                edge_nodes = args
                self.hyperedges[edge_idx] = edge_nodes

            edge_size = len(edge_nodes)
            self.max_cardinality = max(self.max_cardinality, edge_size)

            # Create/update nodes
            for node_id in edge_nodes:
                if node_id not in self.nodes:
                    self.nodes[node_id] = Node(id=node_id)
                self.nodes[node_id].hyperedges.add(edge_idx)

            # Now update connected nodes
            for i in range(len(edge_nodes)):
                for j in range(len(edge_nodes)):
                    if i != j:
                        self.nodes[edge_nodes[i]].connected_nodes.add(edge_nodes[j])
            if not PACE_HGR:
                edge_idx += 1

        # Init buckets with gain range
        for i in range(-2 * self.max_cardinality, 2 * self.max_cardinality + 1):
            self.buckets[0][i] = []
            self.buckets[1][i] = []

        # Random initial partitioning
        for node in self.nodes.values():
            node.partition = random.randrange(2)

        self.gain_init()

        
    def gain_init(self):
        """Recompute gain of each node and reassign buckets accordingly."""
        for p in [0, 1]:
            for g in self.buckets[p]:
                self.buckets[p][g].clear()

        for node in self.nodes.values():
            node.gain = self.count_gain(node)
            self.buckets[node.partition][node.gain].append(node)

    def count_gain(self, node):
        gain = 0
        p = node.partition

        for net in node.hyperedges:
            num_T = 0
            num_F = 0
            for n in self.hyperedges[net]:
                n_part = self.nodes[n].partition
                if n_part == p:
                    num_F += 1
                if n_part == (1-p):
                    num_T += 1
            if num_T == 0:
                gain -=1
            if num_F == 1:
                gain +=1
        return gain

    

    def update_gains(self, moved_node):

            """
                Goals: 
                Update gains of affected buckets only (neighbors only)
            """

            # Update gains for neighbors
            for neighbor_id in moved_node.connected_nodes:
                neighbor = self.nodes[neighbor_id]
                old_gain = neighbor.gain
                if neighbor_id in self.locked_nodes:
                    continue

                # Remove from old gain bucket
                self.buckets[neighbor.partition][old_gain].remove(neighbor)

                # Recalculate gain with new partition configuration
                new_gain = self.count_gain(neighbor)

                # Add to new bucket
                self.buckets[neighbor.partition].setdefault(new_gain, []).append(neighbor)
                neighbor.gain = new_gain
            
            # move node
            self.buckets[moved_node.partition][moved_node.gain].remove(moved_node)
            moved_node.gain = 0 # Set to zero for now.
            moved_node.partition = 1-moved_node.partition
            self.buckets[moved_node.partition].setdefault(moved_node.gain, []).append(moved_node)

    def sort_nodes(self):
        sorted_nodes_info = sorted(
            self.nodes.items(),
            key=lambda item: -item[1].gain
        )
        for node_id, node in sorted_nodes_info:
            yield node, node.partition, node.gain

    def rollback(self, move_stack, move_idx):
        
        for i in range(len(move_stack)-1, move_idx, -1):
            node_id = move_stack[i][0] 
            node = self.nodes[node_id]
            partition = move_stack[i][1]

            # Switch the partition
            # Note that we always put moved nodes in the gain 0 bucket before updating...
            self.buckets[partition][0].remove(node)
            self.buckets[not partition][0].append(node)
            node.partition = not partition

    def partition(self):
        # We want a 50-50 split.
        balance_criteria = math.ceil(self.vertices / 2)
        beta = math.ceil(0.02*self.vertices) # Allow an imbalance factor of 2%.
        prev_gains = []
        gain_sum = 0
        best_gain= float('-inf')
        best_move_idx = -1
        sorted_nodes = self.sort_nodes()
        move_stack = []
        move_index = 0
        while True:
            for node, partition, gain in sorted_nodes:
                if node.id in self.locked_nodes:
                    continue

                # Enforce balance constraint
                new_partition = 1 - partition
                count_new = sum(len(v) for v in self.buckets[new_partition].values())
                count_old = sum(len(v) for v in self.buckets[partition].values())
                if (count_new + 1 > balance_criteria + beta) or (count_old - 1 < balance_criteria + beta):
                    continue

                # Lock node
                self.locked_nodes.add(node.id)

                # Update gains AND move node. 
                self.update_gains(node)
                sorted_nodes = self.sort_nodes()
                
                gain_sum += gain
                # Add move
                move_stack.append((node.id, new_partition))
                if gain_sum > best_gain:
                    best_gain = gain_sum
                    # Save current move stack using an index
                    best_move_idx = move_index
                move_index+=1
            
            prev_gains.append(best_gain)
            self.locked_nodes.clear()

            # Rollback to best config
            best_gain = float('-inf')
            gain_sum = 0

            if len(prev_gains) == 1:
                self.rollback(move_stack, best_move_idx)
                move_stack = []
                move_index = 0
                best_move_idx = 0
                self.gain_init()
                continue
            if prev_gains[-1] <= 0:
                self.rollback(move_stack, 0)
                return self.buckets
            
            # Reset and recalculate gains here
            self.rollback(move_stack, best_move_idx)
            best_move_idx = 0
            move_stack = []
            move_index = 0
            self.gain_init()
            sorted_nodes = self.sort_nodes()
    
    def print_metrics(self, config):
        part_0 = set()
        part_1 = set()

        # Extract nodes from the current config (buckets)
        for bucket in config[0].values():
            part_0.update(bucket)
        for bucket in config[1].values():
            part_1.update(bucket)

        print("Partition 0:", {node.id for node in part_0})
        print("Partition 1:", {node.id for node in part_1})

        edge_cut = 0
        for edge_nodes in self.hyperedges.values():
            seen_partitions = set()
            for node_id in edge_nodes:
                node = self.nodes[node_id]
                seen_partitions.add(node.partition)
                if len(seen_partitions) > 1:
                    edge_cut += 1
                    break

        print(f"Edge cut: {edge_cut}")
        part0_balance = len(part_0)/self.vertices
        part1_balance = len(part_1)/self.vertices
        print(f"Edge balance: {part0_balance:.2f}-{part1_balance:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                            prog='FM.py',
                            description='Implements the Fiduccia-Mattheyses (FM) partitioning algorithm.')
    parser.add_argument('--filename', default='ibm01.hgr', required=True)
    args = parser.parse_args()
    partitioner = FMPartition(args.filename)
    solution = partitioner.partition()
    partitioner.print_metrics(solution)

