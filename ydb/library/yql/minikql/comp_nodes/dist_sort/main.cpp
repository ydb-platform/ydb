#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <condition_variable>
#include <chrono>
#include <map>

#include "algorithms/distributed_partitioning.hpp"
#include "algorithms/helpers.hpp"
#include "algorithms/binary_search.hpp"
#include "algorithms/tlx/sort/strings.hpp"
#include "algorithms/tlx/algorithm.hpp"

using std::cout;
using std::endl;
using std::thread;
using std::map;
using std::setw;

vector<size_t> NODES = { 8 }; // vector of various number of nodes which to run the algorithm for
size_t NUM_NODES = 4; // number of nodes for the current iteration
vector<size_t> factors = { 1 }; // buckets multipliers, ex.: NUM_NODES = 4, factor = 2 -> 8 buckets per node
size_t f = 1; // buckets multiplier for the current iteration
size_t p = f * NUM_NODES; // number of buckets
size_t s = p - 1;
map<size_t, const char*> LEN_TO_FILENAME = { { 20, "data/data_10M_20.txt" }, /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */ };
map<size_t, const char*> LEN_TO_FILENAME_MONSTERS = { { 20, "data/data_10M_20_monsters.txt" }, /* { 50, "data/data_100M_50_monsters.txt" }, { 100, "data/data_100M_100_monsters.txt" } */ };
//const char* filename = "data/data_test_10.txt";
//size_t string_len = 100;

vector<vector<char*>> strings(NUM_NODES);
vector<vector<vector<char*>>> strings_exchanged(NUM_NODES, vector<vector<char*>>(p));
vector<vector<char*>> samples(NUM_NODES, vector<char*>(s));

double network_delay_coeff = 0;
std::mutex m_reading;
std::mutex m_local_partition;
std::mutex m_global_partition;
std::mutex m_strings_exchange;
std::condition_variable cv_reading;
std::condition_variable cv_local_partition;
std::condition_variable cv_global_partition;
std::condition_variable cv_strings_exchange;
size_t threads_finished_reading = 0;
size_t threads_finished_local_partition = 0;
size_t threads_finished_strings_exchange = 0;
bool global_partition_finished = false;

map<timestamp, vector<double>> execution_timestamps;
vector<vector<size_t>> bucket_sizes(NUM_NODES);
vector<char*> splitters(s);

void init(size_t f_, size_t nodes) {
    NUM_NODES = nodes;
    f = f_;
    p = f * nodes;
    s = p - 1;

    for (size_t t = timestamp::DISTRIBUTED_SORT; t <= timestamp::WRITE_DATA; t++) {
        execution_timestamps[static_cast<timestamp>(t)] = vector<double>(nodes);
    }
    strings = vector<vector<char*>>(nodes);
    strings_exchanged = vector<vector<vector<char*>>>(nodes, vector<vector<char*>>(p));
    samples = vector<vector<char*>>(nodes, vector<char*>(s));
    bucket_sizes = vector<vector<size_t>>(nodes);
    splitters = vector<char*>(s);

    threads_finished_reading = 0;
    threads_finished_local_partition = 0;
    threads_finished_strings_exchange = 0;
    global_partition_finished = false;
}

/* Process Emulating One Node */
void node_process(size_t id, char* start, char* end, size_t string_len) {
    /* Step 0 (optional). Read Data */
    auto begin_time_reading_data = std::chrono::high_resolution_clock::now();
    read_data(strings[id], start, end);
    add_timestamp_in_ms(execution_timestamps, timestamp::READ_DATA, begin_time_reading_data, id);

    ++threads_finished_reading;
    // notify when all threads finished reading
    if (threads_finished_reading == NUM_NODES) {
        cv_reading.notify_all();
    }

    {
        // wait until all threads have finished reading data
        std::unique_lock<std::mutex> lock(m_reading);
        cv_reading.wait(lock, [] {
            return threads_finished_reading == NUM_NODES;
        });
    }

    /* Sort Start */
    auto begin_time_distributed_sort = std::chrono::high_resolution_clock::now();

    /* Step 1. MSD Radix Sort */
    cout << "Step 1...\n" << endl;
    auto begin_time_msd_sort = std::chrono::high_resolution_clock::now();

    tlx::sort_strings(strings[id]);
    add_timestamp_in_ms(execution_timestamps, timestamp::MSD_SORT, begin_time_msd_sort, id);

    /* Step 2. Partition Data */
    cout << "Step 2...\n" << endl;
    auto begin_time_local_partition = std::chrono::high_resolution_clock::now();
    partition_cb(strings[id], s, samples[id], string_len);

    add_timestamp_in_ms(execution_timestamps, timestamp::LOCAL_PARTITIONING, begin_time_local_partition, id);

    // notify the leader about finishing local partitioning by all threads
    ++threads_finished_local_partition;
    if (threads_finished_local_partition == NUM_NODES) {
        cv_local_partition.notify_one();
    }

    /* Step 3. Global Partitioning */
    cout << "Step 3...\n" << endl;
    // only the leader is executing this step
    if (id == 0) {
        // wait until all threads have finished local partitioning step
        {
            std::unique_lock<std::mutex> lock(m_local_partition);
            cv_local_partition.wait(lock, [] {
                return threads_finished_local_partition == NUM_NODES;
            });
        }

        auto begin_time_global_partitioning = std::chrono::high_resolution_clock::now();
        vector<char*> sorted_samples;
        for (auto& sample : samples) {
            for (size_t i = 0; i < sample.size(); i++) {
                char* str = sample[i];
                if (strlen(str) != 0) {
                    sorted_samples.push_back(str);
                }
            }
        }

        tlx::sort_strings(sorted_samples);
        partition_cb(sorted_samples, s, splitters, string_len);

        // notify all threads to continue execution
        global_partition_finished = true;
        cv_global_partition.notify_all();
        add_timestamp_in_ms(execution_timestamps, timestamp::GLOBAL_PARTITIONING, begin_time_global_partitioning, id);
    }

    // all threads other than leader are executing the step
    if (id != 0) {
        // wait for the leader to finish *global* partitioning
        auto begin_time_waiting_global_partition = std::chrono::high_resolution_clock::now();
        std::unique_lock<std::mutex> lock(m_global_partition);
        while (!global_partition_finished) {
            cv_global_partition.wait(lock);
        }
        lock.unlock();
        add_timestamp_in_ms(execution_timestamps, timestamp::GLOBAL_PARTITIONING, begin_time_waiting_global_partition, id);
    }

    cout << "Step 4...\n" << endl;
    /* Step 4. Buckets Determination */
    vector<size_t> partition_idx;
    auto begin_time_buckets_determination_timestamps = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < p - 1; ++i) {
        std::pair<size_t, bool> res = binary_search(strings[id], splitters[i]);
        if (res.second) {
            partition_idx.emplace_back(res.first);
        } else {
            partition_idx.emplace_back(res.first - 1);
        }
    }
    partition_idx.emplace_back(strings[id].size() - 1);
    add_timestamp_in_ms(execution_timestamps, timestamp::BUCKETS_DETERMINATION, begin_time_buckets_determination_timestamps, id);

    /* Step 5. Strings Exchange */
    cout << "Step 5...\n" << endl;
    auto begin_time_strings_exchange = std::chrono::high_resolution_clock::now();
    size_t prev_idx = -1;

    for (size_t i = 0; i < partition_idx.size(); i++) {
        size_t idx = partition_idx[i];

        vector<char*>::const_iterator first = strings[id].begin() + prev_idx + 1;
        vector<char*>::const_iterator last = strings[id].begin() + idx + 1;

        // emulate network delay depending on the size of the strings (uncomment if needed)
//        double sleep_mcrs = network_delay_coeff * (last - first);
//        usleep(sleep_mcrs);

        size_t node_id = i / f;
        size_t bucket_id = (f * id) + (i % f);
        strings_exchanged[node_id][bucket_id] = vector<char*>(first, last);
        prev_idx = idx;
    }
    add_timestamp_in_ms(execution_timestamps, timestamp::STRINGS_EXCHANGE, begin_time_strings_exchange, id);

    // notify 'Strings Exchange' step finished
    ++threads_finished_strings_exchange;
    if (threads_finished_strings_exchange == NUM_NODES) {
        cv_strings_exchange.notify_all();
    }

    // wait until all threads have finished 'Strings Exchange' step
    auto begin_time_strings_exchange_waiting = std::chrono::high_resolution_clock::now();
    {
        std::unique_lock<std::mutex> lock(m_strings_exchange);
        while (threads_finished_strings_exchange != NUM_NODES) {
            cv_strings_exchange.wait(lock);
        }
    }

    // save sizes of all buckets
    for (int i = 0; i < strings_exchanged[id].size(); i++){
        bucket_sizes[id].push_back(strings_exchanged[id][i].size());
    }

    add_timestamp_in_ms(execution_timestamps, timestamp::STRINGS_EXCHANGE_WAITING, begin_time_strings_exchange_waiting, id);

    /* Step 6. Local Merge */
    cout << "Step 6...\n" << endl;
    auto begin_time_local_merge = std::chrono::high_resolution_clock::now();

    size_t total_size = 0;
    for (const auto& vec : strings_exchanged[id]) {
        total_size += vec.size();
    }
    vector<char*> sorted_strings(total_size);
    std::vector<std::pair<std::vector<char*>::iterator, std::vector<char*>::iterator>> sequences;
    for (auto& vec : strings_exchanged[id]) {
        sequences.emplace_back(vec.begin(), vec.end());
    }
    auto comp = [&string_len](const char* a, const char* b) {
        return std::memcmp(a, b, string_len) < 0;
    };
    tlx::parallel_multiway_merge(sequences.begin(), sequences.end(), sorted_strings.begin(), total_size, comp);
    /* tlx::multiway_merge(sequences.begin(), sequences.end(), sorted_strings.begin(), total_size, comp); */

    add_timestamp_in_ms(execution_timestamps, timestamp::LOCAL_MERGE, begin_time_local_merge, id);

    // distributed sort finished
    add_timestamp_in_ms(execution_timestamps, timestamp::DISTRIBUTED_SORT, begin_time_distributed_sort, id);

    cout << "Finished...\n" << endl;
    for (auto s : strings[id]) {
        delete[] s;
    }

    // output sorted local strings to files
//    auto begin_time_write_data = std::chrono::high_resolution_clock::now();
//    std::ofstream out("out" + std::to_string(id) + ".txt");
//    for (auto& str : sorted_strings) {
//        out << str << '\n';
//    }
//    out.close();
//    add_timestamp_in_ms(execution_timestamps, timestamp::WRITE_DATA, begin_time_write_data, id);
}

int main() {
    // uncomment if you need to generate data files
//    fill_data_with_key_monsters();
//    fill_data();

    for (auto nodes : NODES) {
        // run for various types of data
        for (auto len_filename: LEN_TO_FILENAME_MONSTERS) {
            size_t string_len = len_filename.first;
            const char *filename = len_filename.second;

            // run for different number of buckets
            for (size_t f_: factors) {
                init(f_, nodes);
                // map file to memory
                vector<thread> nodes;

                struct stat sb;
                size_t fd = open(filename, O_RDONLY);
                if (fstat(fd, &sb) == -1) {
                    perror("Error getting file size");
                    close(fd);
                    return 1;
                }

                char *mapped = static_cast<char *>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
                if (mapped == MAP_FAILED) {
                    perror("Error mapping file");
                    close(fd);
                    return 1;
                }

                /* Single Thread Test */
                cout << "Single Thread Started...\n" << endl;
                vector<char *> single_thread_strings;
                char* start = mapped;
                char* end = mapped + sb.st_size;
                read_data(single_thread_strings, start, end);

                std::chrono::high_resolution_clock::time_point begin_time = std::chrono::high_resolution_clock::now();
                tlx::sort_strings(single_thread_strings);
                auto end_time = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::milli> duration = end_time - begin_time;
                for (auto s : single_thread_strings) {
                    delete[] s;
                }
                double time_ms = duration.count();
                double bytes_in_mb = 1'048'576;
                double speed = filesize(filename) / bytes_in_mb / (time_ms / 1000);

                /* Multiple Threads Test */
                size_t part_size = sb.st_size / NUM_NODES;

                // run threads
                cout << "Distributed Sorting Started...\n" << endl;
                for (size_t i = 0; i < NUM_NODES; i++) {
                    char *part_start = mapped + i * part_size;
                    char *part_end = (i == NUM_NODES - 1) ? mapped + sb.st_size : part_start + part_size;
                    nodes.emplace_back(node_process, i, part_start, part_end, string_len);
                }

                for (auto &n: nodes) {
                    n.join();
                }

                munmap(mapped, sb.st_size);
                close(fd);

                output_statistics(filename, execution_timestamps, bucket_sizes, p, NUM_NODES, string_len, time_ms,
                                  speed);
            }
        }
    }
    return 0;
}
