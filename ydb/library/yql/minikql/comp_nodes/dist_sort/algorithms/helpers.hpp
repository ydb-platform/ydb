#pragma once

#include <cmath>
#include <random>
#include <algorithm>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <unordered_set>
#include <iomanip>
#include <fstream>

using namespace std;

enum timestamp {
    DISTRIBUTED_SORT,
    MSD_SORT,
    LOCAL_MERGE,
    LOCAL_PARTITIONING,
    GLOBAL_PARTITIONING,
    BUCKETS_DETERMINATION,
    STRINGS_EXCHANGE,
    STRINGS_EXCHANGE_WAITING,
    READ_DATA,
    WRITE_DATA
};

map<timestamp, string> TIMESTAMP_NAMES = {
        { timestamp::DISTRIBUTED_SORT, "Distributed Sort" },
        { timestamp::MSD_SORT, "MSD Sort" },
        { timestamp::LOCAL_MERGE, "Local Merge" },
        { timestamp::LOCAL_PARTITIONING, "Local Partitioning" },
        { timestamp::GLOBAL_PARTITIONING, "Global Partitioning" },
        { timestamp::BUCKETS_DETERMINATION, "Buckets Determination" },
        { timestamp::STRINGS_EXCHANGE, "Strings Exchange" },
        { timestamp::STRINGS_EXCHANGE_WAITING, "Strings Exchange Waiting" },
        { timestamp::READ_DATA, "Reading Data" },
        { timestamp::WRITE_DATA, "Writing Data" }
};

void read_data(vector<char*>& data, char* start, char* end) {
    while (start < end) {
        char* line_end = strchr(start, '\n');
        if (line_end == nullptr || line_end > end) {
            line_end = end;
        }

        char* line = new char[line_end - start];
        memcpy(line, start, line_end - start);
        if (line_end - start != 0) {
            data.push_back(line);
        }

        if (line_end == end) {
            break;
        }

        start = line_end + 1;
    }
}

std::string generate_random_string(size_t length, std::mt19937& gen) {
    std::uniform_int_distribution<> dis(0, 9);
    std::string str(length, '0');
    for (size_t i = 0; i < length; ++i) {
        str[i] = '0' + dis(gen);
    }
    return str;
}


void fill_data() {
    srand((unsigned)time(NULL));
    size_t strings_num = 100'000'000;
    vector<size_t> STR_LENGTHS = { 20, 50, 100 };

    std::random_device rd;
    std::mt19937 gen(rd());

    for (size_t len : STR_LENGTHS) {
        std::ofstream file("data_" + std::to_string(strings_num / 1'000'000) + "M_" + std::to_string(len) + ".txt");
        vector<string> data_file(strings_num);
        for (size_t i = 0; i < strings_num; i++) {
            data_file[i] = generate_random_string(len, gen);
        }

        for (auto str: data_file) {
            file << str;
            file << std::endl;
        }

        file.close();
    }
}

std::vector<std::string> generate_strings_with_keys_monsters(size_t string_length, size_t strings_num) {
    size_t num_unique_strings = strings_num / 2;
    size_t num_repeated_strings = 10;
    size_t repeat_count = (strings_num - num_unique_strings) / num_repeated_strings;

    // Seed the random number generator
    std::random_device rd;
    std::mt19937 gen(rd());

    // Set to keep track of unique strings
    std::unordered_set<std::string> unique_strings;

    // Vector to store the generated strings
    std::vector<std::string> result;
    result.reserve(strings_num);

    // Generate num_unique_strings unique strings
    while (unique_strings.size() < num_unique_strings) {
        std::string new_string = generate_random_string(string_length, gen);
        if (unique_strings.insert(new_string).second) {
            result.push_back(new_string);
        }
    }

    // Generate num_repeated_strings unique strings each repeated repeat_count times
    std::unordered_set<std::string> repeated_strings;
    while (repeated_strings.size() < num_repeated_strings) {
        std::string new_string = generate_random_string(string_length, gen);
        if (repeated_strings.insert(new_string).second) {
            for (size_t i = 0; i < repeat_count; ++i) {
                result.push_back(new_string);
            }
        }
    }

    return result;
}

void fill_data_with_key_monsters() {
    size_t strings_num = 100'000'000;
    vector<size_t> STR_LENGTHS = { 20, 50, 100 };

    for (size_t len : STR_LENGTHS) {
        std::ofstream file("data_" + std::to_string(strings_num / 1'000'000) + "M_" + std::to_string(len) + "_monsters.txt");

        vector<string> data_file = generate_strings_with_keys_monsters(len, strings_num);

        for (const auto& str: data_file) {
            file << str;
            file << std::endl;
        }

        file.close();
    }
}

template <class T>
void print_vector(vector<T>& v, char sep = '\n') {
    for (auto& el : v) {
        cout << el << sep;
    }
}

std::ifstream::pos_type filesize(const char* filename) {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

void add_timestamp_in_ms(map<timestamp, vector<double>>& execution_timestamps, timestamp timestamp_name, std::chrono::high_resolution_clock::time_point begin_time, size_t id) {
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - begin_time;
    double time_ms_sort = duration.count();
    execution_timestamps[timestamp_name][id] = time_ms_sort;
}

void report_time(std::chrono::high_resolution_clock::time_point begin_time, const char* filename) {
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - begin_time;
    double time_ms = duration.count();
    std::cout << "Single Thread Sorting Time: " << time_ms << "[ms]" << std::endl;
    double bytes_in_mb = 1'048'576;
    double speed = filesize(filename) / bytes_in_mb / (time_ms / 1000);
    std::cout << "Single Thread Speed: " << speed << "[MB/s]\n" << endl;
}

// TODO: refactor
void output_statistics(const char* FILENAME, const map<timestamp, vector<double>>& data, const vector<vector<size_t>> bucket_sizes, int p, int num_nodes, size_t string_len,
                       double time_ms, double speed) {
    std::ofstream result("results/" + std::to_string(num_nodes) + "_nodes_" + std::to_string(p / num_nodes) + "_factor_" + static_cast<string>(FILENAME));
    result << "Single Thread Sorting Time: " << time_ms << "[ms]" << std::endl;
    result << "Single Thread Speed: " << speed << "[MB/s]\n" << endl;

    // Determine the number of rows and columns
    int rows = data.size();
    int cols = 0;
    size_t table_len = 247;

    cols = data.begin()->second.size();

    vector<double> node_sizes(bucket_sizes.size());
    for (int i = 0; i < bucket_sizes.size(); i++) {
        for (int j = 0; j < bucket_sizes[i].size(); j++) {
            node_sizes[i] += bucket_sizes[i][j];
        }
    }

    // Print the headers (row names)
    int f = 0;
    size_t first_col_size = 20;
    for (const auto& entry : data) {
        if (f == 0) {
            result << setw(first_col_size) << " ";
            f++;
        }
        string col = TIMESTAMP_NAMES[entry.first] + ", ms |";
        result << setw(col.size() + 1) << col;
    }
    result << endl << string(table_len, '-') << endl;

    // Print the data
    vector<int> first_col_width(cols);
    for (int i = 0; i < cols; ++i) {
        string row_name = "Node " + std::to_string(i + 1) + " (" + std::to_string((int)node_sizes[i]) + ")";
        first_col_width.push_back(row_name.size());
        result << row_name << setw(first_col_size - row_name.size()) << "|";

        for (const auto& entry : data) {
            result << setw(TIMESTAMP_NAMES[entry.first].size() + 5) << std::fixed << std::setprecision(2) << entry.second[i] << " |";
        }
        result << endl;
    }

    double data_size_bytes = filesize(FILENAME);
    const size_t bytes_in_mb = 1'048'576;
    double whole_data_size_mb = data_size_bytes / bytes_in_mb;
    double msd_sort_data_size_mb = whole_data_size_mb / num_nodes;
    vector<double> sizes = { whole_data_size_mb, msd_sort_data_size_mb };
    result << string(table_len, '=') << endl;

    string row_name = "Time max, ms";
    result << row_name << setw(first_col_size - row_name.size()) << "|";

    for (const auto& entry : data) {
        vector<double> v = entry.second;
        double max_time_ms = *std::max_element(v.begin(), v.end());
        result << setw(TIMESTAMP_NAMES[entry.first].size() + 5) << std::fixed << std::setprecision(2) << max_time_ms << " |";
    }

    row_name = "Speed, MB/s";
    result << endl << row_name << setw(first_col_size - row_name.size()) << "|";

    // speed ==
    //  - for distributed sort: size of file / max time
    //  - for msd sort: size of node / max time
    //  - for local merge: size of node after strings exchange / max time
    int j = 0;
    for (int i = timestamp::DISTRIBUTED_SORT; i <= timestamp::LOCAL_MERGE; i++) {
        timestamp t = static_cast<timestamp>(i);
        double size_mb = sizes[j++];
        vector<double> v = data.at(t);
        double max_time_ms = 0;
        double max_time_i = 0;
        for (int k = 0; k < v.size(); k++) {
            if (v[k] > max_time_ms) {
                max_time_ms = v[k];
                max_time_i = k;
            }
        }
        double max_time_s = max_time_ms / 1000;
        double speed_mb_s = size_mb / max_time_s;
        if (t == timestamp::LOCAL_MERGE) {
            speed_mb_s = node_sizes[max_time_i] * (string_len + 1) / bytes_in_mb / max_time_s;
        }
        result << setw(TIMESTAMP_NAMES[t].size() + 5) << std::fixed << std::setprecision(2) << speed_mb_s << " |";
    }
    result << endl << endl;

    result.close();
}