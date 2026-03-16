/******************************************************************************
 * Project:  PROJ
 * Purpose:  Downloader tool
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2020, Even Rouault, <even.rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

//! @cond Doxygen_Suppress

#define FROM_PROJ_CPP

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <set>
#include <string>

#include "filemanager.hpp"
#include "proj.h"
#include "proj_internal.h"

#include "proj/internal/include_nlohmann_json.hpp"
#include "proj/internal/internal.hpp"

using json = nlohmann::json;
using namespace NS_PROJ::internal;

// ---------------------------------------------------------------------------

namespace {
class ParsingException : public std::exception {
    std::string msg_;

  public:
    explicit ParsingException(const char *msg) : msg_(msg) {}
    const char *what() const noexcept override { return msg_.c_str(); }
};
} // namespace

// ---------------------------------------------------------------------------

[[noreturn]] static void usage() {
    std::cerr << "usage: projsync " << std::endl;
    std::cerr << "          [--endpoint URL]" << std::endl;
    std::cerr << "          [--local-geojson-file FILENAME]" << std::endl;
    std::cerr << "          ([--user-writable-directory] | "
                 "[--system-directory] | [--target-dir DIRNAME])"
              << std::endl;
    std::cerr << "          [--bbox west_long,south_lat,east_long,north_lat]"
              << std::endl;
    std::cerr << "          [--spatial-test contains|intersects]" << std::endl;
    std::cerr << "          [--source-id ID] [--area-of-use NAME]" << std::endl;
    std::cerr << "          [--file NAME]" << std::endl;
    std::cerr << "          [--all] [--exclude-world-coverage]" << std::endl;
    std::cerr << "          [--quiet | --verbose] [--dry-run] [--list-files]"
              << std::endl;
    std::cerr << "          [--no-version-filtering]" << std::endl;
    std::exit(1);
}

// ---------------------------------------------------------------------------

static std::vector<double> get_bbox(const json &j) {
    std::vector<double> res;
    if (j.size() == 2 && j[0].is_number() && j[1].is_number()) {
        res.push_back(j[0].get<double>());
        res.push_back(j[1].get<double>());
        res.push_back(j[0].get<double>());
        res.push_back(j[1].get<double>());
    } else {
        for (const auto &obj : j) {
            if (obj.is_array()) {
                auto subres = get_bbox(obj);
                if (subres.size() == 4) {
                    if (res.empty()) {
                        res = std::move(subres);
                    } else {
                        res[0] = std::min(res[0], subres[0]);
                        res[1] = std::min(res[1], subres[1]);
                        res[2] = std::max(res[2], subres[2]);
                        res[3] = std::max(res[3], subres[3]);
                    }
                }
            }
        }
    }
    return res;
}

// ---------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    pj_stderr_proj_lib_deprecation_warning();

    auto ctx = pj_get_default_ctx();

    std::string targetDir;
    std::string endpoint(proj_context_get_url_endpoint(ctx));
    const std::string geojsonFile("files.geojson");
    std::string queriedSourceId;
    std::string queriedAreaOfUse;
    bool listFiles = false;
    bool dryRun = false;
    bool hasQueriedBbox = false;
    double queried_west = 0.0;
    double queried_south = 0.0;
    double queried_east = 0.0;
    double queried_north = 0.0;
    bool intersects = true;
    bool quiet = false;
    bool verbose = false;
    bool includeWorldCoverage = true;
    bool queryAll = false;
    std::string queriedFilename;
    std::string files_geojson_local;
    bool versionFiltering = true;

    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);
        if (arg == "--endpoint" && i + 1 < argc) {
            i++;
            endpoint = argv[i];
        } else if (arg == "--user-writable-directory") {
            // do nothing
        } else if (arg == "--system-directory") {
            targetDir = pj_get_relative_share_proj(ctx);
#ifdef PROJ_DATA
            if (targetDir.empty()) {
                targetDir = PROJ_DATA;
            }
#endif
        } else if (arg == "--target-dir" && i + 1 < argc) {
            i++;
            targetDir = argv[i];
        } else if (arg == "--local-geojson-file" && i + 1 < argc) {
            i++;
            files_geojson_local = argv[i];
        } else if (arg == "--list-files") {
            listFiles = true;
        } else if (arg == "--source-id" && i + 1 < argc) {
            i++;
            queriedSourceId = argv[i];
        } else if (arg == "--area-of-use" && i + 1 < argc) {
            i++;
            queriedAreaOfUse = argv[i];
        } else if (arg == "--file" && i + 1 < argc) {
            i++;
            queriedFilename = argv[i];
        } else if (arg == "--bbox" && i + 1 < argc) {
            i++;
            auto bboxStr(argv[i]);
            auto bbox(split(bboxStr, ','));
            if (bbox.size() != 4) {
                std::cerr << "Incorrect number of values for option --bbox: "
                          << bboxStr << std::endl;
                usage();
            }
            try {
                queried_west = c_locale_stod(bbox[0]);
                queried_south = c_locale_stod(bbox[1]);
                queried_east = c_locale_stod(bbox[2]);
                queried_north = c_locale_stod(bbox[3]);
            } catch (const std::exception &e) {
                std::cerr << "Invalid value for option --bbox: " << bboxStr
                          << ", " << e.what() << std::endl;
                usage();
            }
            if (queried_west > 180 && queried_east > queried_west) {
                queried_west -= 360;
                queried_east -= 360;
            } else if (queried_west < -180 && queried_east > queried_west) {
                queried_west += 360;
                queried_east += 360;
            } else if (fabs(queried_west) < 180 && fabs(queried_east) < 180 &&
                       queried_east < queried_west) {
                queried_east += 360;
            }
            hasQueriedBbox = true;
        } else if (arg == "--spatial-test" && i + 1 < argc) {
            i++;
            const std::string value(argv[i]);
            if (ci_equal(value, "contains")) {
                intersects = false;
            } else if (ci_equal(value, "intersects")) {
                intersects = true;
            } else {
                std::cerr << "Unrecognized value for option --spatial-test: "
                          << value << std::endl;
                usage();
            }
        } else if (arg == "--dry-run") {
            dryRun = true;
        } else if (arg == "--exclude-world-coverage") {
            includeWorldCoverage = false;
        } else if (arg == "--all") {
            queryAll = true;
        } else if (arg == "--no-version-filtering") {
            versionFiltering = false;
        } else if (arg == "-q" || arg == "--quiet") {
            quiet = true;
        } else if (arg == "--verbose") {
            verbose = true;
        } else {
            usage();
        }
    }
    if (!listFiles && queriedFilename.empty() && queriedSourceId.empty() &&
        queriedAreaOfUse.empty() && !hasQueriedBbox && !queryAll) {
        std::cerr << "At least one of --list-files, --file, --source-id, "
                     "--area-of-use, --bbox or --all must be specified."
                  << std::endl
                  << std::endl;
        usage();
    }

    if (targetDir.empty()) {
        targetDir = proj_context_get_user_writable_directory(ctx, true);
    } else {
        if (targetDir.back() == '/') {
            targetDir.resize(targetDir.size() - 1);
        }

        // This is used by projsync() to determine where to write files.
        proj_context_set_user_writable_directory(ctx, targetDir.c_str(), true);
    }

    if (!endpoint.empty() && endpoint.back() == '/') {
        endpoint.resize(endpoint.size() - 1);
    }

    if (!quiet && !listFiles) {
        std::cout << "Downloading from " << endpoint << " into " << targetDir
                  << std::endl;
    }

    proj_context_set_enable_network(ctx, true);
    if (files_geojson_local.empty()) {
        const std::string files_geojson_url(endpoint + '/' + geojsonFile);
        if (!proj_download_file(ctx, files_geojson_url.c_str(), false, nullptr,
                                nullptr)) {
            std::cerr << "Cannot download " << geojsonFile << std::endl;
            std::exit(1);
        }

        files_geojson_local = targetDir + '/' + geojsonFile;
    }

    auto file = NS_PROJ::FileManager::open(ctx, files_geojson_local.c_str(),
                                           NS_PROJ::FileAccess::READ_ONLY);
    if (!file) {
        std::cerr << "Cannot open " << files_geojson_local << std::endl;
        std::exit(1);
    }

    std::string text;
    while (true) {
        bool maxLenReached = false;
        bool eofReached = false;
        text += file->read_line(1000000, maxLenReached, eofReached);
        if (maxLenReached) {
            std::cerr << "Error while parsing " << geojsonFile
                      << " : too long line" << std::endl;
            std::exit(1);
        }
        if (eofReached)
            break;
    }
    file.reset();

    if (listFiles) {
        std::cout << "filename,area_of_use,source_id,file_size" << std::endl;
    }

    std::string proj_data_version_str;
    int proj_data_version_major = 0;
    int proj_data_version_minor = 0;
    {
        const char *proj_data_version =
            proj_context_get_database_metadata(ctx, "PROJ_DATA.VERSION");
        if (proj_data_version) {
            proj_data_version_str = proj_data_version;
            const auto tokens = split(proj_data_version, '.');
            if (tokens.size() >= 2) {
                proj_data_version_major = atoi(tokens[0].c_str());
                proj_data_version_minor = atoi(tokens[1].c_str());
            }
        }
    }

    try {
        const auto j = json::parse(text);
        bool foundMatchSourceIdCriterion = false;
        std::set<std::string> source_ids;
        bool foundMatchAreaOfUseCriterion = false;
        std::set<std::string> areas_of_use;
        bool foundMatchFileCriterion = false;
        std::set<std::string> files;
        if (!j.is_object() || !j.contains("features")) {
            throw ParsingException("no features member");
        }
        std::vector<std::string> to_download;
        unsigned long long total_size_to_download = 0;
        const auto features = j["features"];
        for (const auto &feat : features) {
            if (!feat.is_object()) {
                continue;
            }
            if (!feat.contains("properties")) {
                continue;
            }
            const auto properties = feat["properties"];
            if (!properties.is_object()) {
                continue;
            }

            if (!properties.contains("name")) {
                continue;
            }
            const auto j_name = properties["name"];
            if (!j_name.is_string()) {
                continue;
            }
            const auto name(j_name.get<std::string>());

            if (versionFiltering && proj_data_version_major > 0 &&
                properties.contains("version_added")) {
                const auto j_version_added = properties["version_added"];
                if (j_version_added.is_string()) {
                    const auto version_added(
                        j_version_added.get<std::string>());
                    const auto tokens = split(version_added, '.');
                    if (tokens.size() >= 2) {
                        int version_major = atoi(tokens[0].c_str());
                        int version_minor = atoi(tokens[1].c_str());
                        if (proj_data_version_major < version_major ||
                            (proj_data_version_major == version_major &&
                             proj_data_version_minor < version_minor)) {
                            // File only useful for a later PROJ version
                            if (verbose) {
                                std::cout << "Skipping " << name
                                          << " as it is only useful starting "
                                             "with PROJ-data "
                                          << version_added
                                          << " and we are targeting "
                                          << proj_data_version_str << std::endl;
                            }
                            continue;
                        }
                    }
                }
            }

            if (versionFiltering && proj_data_version_major > 0 &&
                properties.contains("version_removed")) {
                const auto j_version_removed = properties["version_removed"];
                if (j_version_removed.is_string()) {
                    const auto version_removed(
                        j_version_removed.get<std::string>());
                    const auto tokens = split(version_removed, '.');
                    if (tokens.size() >= 2) {
                        int version_major = atoi(tokens[0].c_str());
                        int version_minor = atoi(tokens[1].c_str());
                        if (proj_data_version_major > version_major ||
                            (proj_data_version_major == version_major &&
                             proj_data_version_minor >= version_minor)) {
                            // File only useful for a previous PROJ version
                            if (verbose) {
                                std::cout << "Skipping " << name
                                          << " as it is no longer useful "
                                             "starting with PROJ-data "
                                          << version_removed
                                          << " and we are targeting "
                                          << proj_data_version_str << std::endl;
                            }
                            continue;
                        }
                    }
                }
            }

            files.insert(name);

            if (!properties.contains("source_id")) {
                continue;
            }
            const auto j_source_id = properties["source_id"];
            if (!j_source_id.is_string()) {
                continue;
            }
            const auto source_id(j_source_id.get<std::string>());
            source_ids.insert(source_id);

            std::string area_of_use;
            if (properties.contains("area_of_use")) {
                const auto j_area_of_use = properties["area_of_use"];
                if (j_area_of_use.is_string()) {
                    area_of_use = j_area_of_use.get<std::string>();
                    areas_of_use.insert(area_of_use);
                }
            }

            unsigned long long file_size = 0;
            if (properties.contains("file_size")) {
                const auto j_file_size = properties["file_size"];
                if (j_file_size.type() == json::value_t::number_unsigned) {
                    file_size = j_file_size.get<unsigned long long>();
                }
            }

            const bool matchSourceId =
                queryAll || queriedSourceId.empty() ||
                source_id.find(queriedSourceId) != std::string::npos;
            if (!queriedSourceId.empty() &&
                source_id.find(queriedSourceId) != std::string::npos) {
                foundMatchSourceIdCriterion = true;
            }

            const bool matchAreaOfUse =
                queryAll || queriedAreaOfUse.empty() ||
                area_of_use.find(queriedAreaOfUse) != std::string::npos;
            if (!queriedAreaOfUse.empty() &&
                area_of_use.find(queriedAreaOfUse) != std::string::npos) {
                foundMatchAreaOfUseCriterion = true;
            }

            const bool matchFile =
                queryAll || queriedFilename.empty() ||
                name.find(queriedFilename) != std::string::npos;
            if (!queriedFilename.empty() &&
                name.find(queriedFilename) != std::string::npos) {
                foundMatchFileCriterion = true;
            }

            bool matchBbox = true;
            if (queryAll || hasQueriedBbox) {
                matchBbox = false;
                do {
                    if (!feat.contains("geometry")) {
                        if (queryAll) {
                            matchBbox = true;
                        }
                        break;
                    }
                    const auto j_geometry = feat["geometry"];
                    if (!j_geometry.is_object()) {
                        if (queryAll) {
                            matchBbox = true;
                        }
                        break;
                    }
                    if (!j_geometry.contains("coordinates")) {
                        break;
                    }
                    const auto j_coordinates = j_geometry["coordinates"];
                    if (!j_coordinates.is_array()) {
                        break;
                    }
                    if (!j_geometry.contains("type")) {
                        break;
                    }
                    const auto j_geometry_type = j_geometry["type"];
                    if (!j_geometry_type.is_string()) {
                        break;
                    }
                    const auto geometry_type(
                        j_geometry_type.get<std::string>());
                    std::vector<double> grid_bbox;
                    if (geometry_type == "MultiPolygon") {
                        std::vector<std::vector<double>> grid_bboxes;
                        bool foundMinus180 = false;
                        bool foundPlus180 = false;
                        for (const auto &obj : j_coordinates) {
                            if (obj.is_array()) {
                                auto tmp = get_bbox(obj);
                                if (tmp.size() == 4) {
                                    if (tmp[0] == -180)
                                        foundMinus180 = true;
                                    else if (tmp[2] == 180)
                                        foundPlus180 = true;
                                    grid_bboxes.push_back(std::move(tmp));
                                }
                            }
                        }
                        for (auto &bbox : grid_bboxes) {
                            if (foundMinus180 && foundPlus180 &&
                                bbox[0] == -180) {
                                bbox[0] = 180;
                                bbox[2] += 360;
                            }
                            if (grid_bbox.empty()) {
                                grid_bbox = bbox;
                            } else {
                                grid_bbox[0] = std::min(grid_bbox[0], bbox[0]);
                                grid_bbox[1] = std::min(grid_bbox[1], bbox[1]);
                                grid_bbox[2] = std::max(grid_bbox[2], bbox[2]);
                                grid_bbox[3] = std::max(grid_bbox[3], bbox[3]);
                            }
                        }
                    } else {
                        grid_bbox = get_bbox(j_coordinates);
                    }
                    if (grid_bbox.size() != 4) {
                        break;
                    }
                    double grid_w = grid_bbox[0];
                    const double grid_s = grid_bbox[1];
                    double grid_e = grid_bbox[2];
                    const double grid_n = grid_bbox[3];
                    if (grid_e - grid_w > 359 && grid_n - grid_s > 179) {
                        if (!includeWorldCoverage) {
                            break;
                        }
                        grid_w = -std::numeric_limits<double>::max();
                        grid_e = std::numeric_limits<double>::max();
                    } else if (grid_e > 180 && queried_west < -180) {
                        grid_w -= 360;
                        grid_e -= 360;
                    }
                    if (queryAll) {
                        matchBbox = true;
                        break;
                    }

                    if (intersects) {
                        if (queried_west < grid_e && grid_w < queried_east &&
                            queried_south < grid_n && grid_s < queried_north) {
                            matchBbox = true;
                        }
                    } else {
                        if (grid_w >= queried_west && grid_s >= queried_south &&
                            grid_e <= queried_east && grid_n <= queried_north) {
                            matchBbox = true;
                        }
                    }

                } while (false);
            }

            if (matchFile && matchSourceId && matchAreaOfUse && matchBbox) {

                if (listFiles) {
                    std::cout << name << "," << area_of_use << "," << source_id
                              << "," << file_size << std::endl;
                    continue;
                }

                std::string resource_url(
                    std::string(endpoint).append("/").append(name));
                if (proj_is_download_needed(ctx, resource_url.c_str(), false)) {
                    total_size_to_download += file_size;
                    to_download.push_back(std::move(resource_url));
                } else {
                    if (!quiet) {
                        std::cout << resource_url << " already downloaded."
                                  << std::endl;
                    }
                }
            }
        }

        if (!quiet && !listFiles && total_size_to_download > 0) {
            if (total_size_to_download > 1024 * 1024)
                std::cout << "Total size to download: "
                          << total_size_to_download / (1024 * 1024) << " MB"
                          << std::endl;
            else
                std::cout << "Total to download: " << total_size_to_download
                          << " bytes" << std::endl;
        }
        for (size_t i = 0; i < to_download.size(); ++i) {
            const auto &url = to_download[i];
            if (!quiet) {
                if (dryRun) {
                    std::cout << "Would download ";
                } else {
                    std::cout << "Downloading ";
                }
                std::cout << url << "... (" << i + 1 << " / "
                          << to_download.size() << ")" << std::endl;
            }
            if (!dryRun && !proj_download_file(ctx, url.c_str(), false, nullptr,
                                               nullptr)) {
                std::cerr << "Cannot download " << url << std::endl;
                std::exit(1);
            }
        }

        if (!queriedSourceId.empty() && !foundMatchSourceIdCriterion) {
            std::cerr << "Warning: '" << queriedSourceId
                      << "' is a unknown value for --source-id." << std::endl;
            std::cerr << "Known values are:" << std::endl;
            for (const auto &v : source_ids) {
                std::cerr << "  " << v << std::endl;
            }
            std::exit(1);
        }

        if (!queriedAreaOfUse.empty() && !foundMatchAreaOfUseCriterion) {
            std::cerr << "Warning: '" << queriedAreaOfUse
                      << "' is a unknown value for --area-of-use." << std::endl;
            std::cerr << "Known values are:" << std::endl;
            for (const auto &v : areas_of_use) {
                std::cerr << "  " << v << std::endl;
            }
            std::exit(1);
        }

        if (!queriedFilename.empty() && !foundMatchFileCriterion) {
            std::cerr << "Warning: '" << queriedFilename
                      << "' is a unknown value for --file." << std::endl;
            std::cerr << "Known values are:" << std::endl;
            for (const auto &v : files) {
                std::cerr << "  " << v << std::endl;
            }
            std::exit(1);
        }
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        std::exit(1);
    }

    return 0;
}

//! @endcond
