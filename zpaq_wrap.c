#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <glob.h>
#include <string.h>

#define PAGE_SIZE 4096  // 4KB

void print_usage() {
    printf("Usage: ./ztester <chunksize> <dumpdirectory> <outputfile>\n");
    printf("  <chunksize>    - Buffer size index for compression (choices: 0, 1, 2, 3, 4)\n");
    printf("  <dumpdirectory> - Directory containing dump files to process\n");
    printf("  <outputfile>    - Output file to save results\n");
    printf("\nBuffer size mapping:\n");
    printf("  0 - 16384 bytes\n");
    printf("  1 - 65536 bytes\n");
    printf("  2 - 262144 bytes\n");
    printf("  3 - 1048576 bytes\n");
    printf("  4 - 2097152 bytes\n");
}

long compress_and_measure(const char* input_file, int buffsize, long* effective_size_out) {
    FILE* input = fopen(input_file, "rb");
    if (!input) {
        perror("Error opening input file");
        return 0;
    }

    // Extract filename only
    const char* filename = strrchr(input_file, '/');
    filename = (filename) ? filename + 1 : input_file;

    // Allocate reasonable size for file paths
    char temp_file[512];
    char compressed_file[512];

    snprintf(temp_file, sizeof(temp_file), "testdir/%s_temp_chunk_%d.bin", filename, buffsize);
    snprintf(compressed_file, sizeof(compressed_file), "testdir/%s_temp_chunk_%d.zpaq", filename, buffsize);

    // Calculate total size and effective size (minus zero pages)
    fseek(input, 0, SEEK_END);
    long total_size = ftell(input);
    fseek(input, 0, SEEK_SET);

    if (total_size == 0) {
        printf("Input file is empty: %s\n", input_file);
        fclose(input);
        *effective_size_out = 0;
        return 0;
    }

    long zero_pages_count = 0;
    unsigned char page_buffer[PAGE_SIZE];
    size_t bytes_read;
    long effective_size = total_size;

    while ((bytes_read = fread(page_buffer, 1, PAGE_SIZE, input)) > 0) {
        if (bytes_read < PAGE_SIZE) {
            memset(page_buffer + bytes_read, 0, PAGE_SIZE - bytes_read);
        }

        int is_zero = 1;
        for (size_t i = 0; i < PAGE_SIZE; ++i) {
            if (page_buffer[i] != 0) {
                is_zero = 0;
                break;
            }
        }
        if (is_zero) {
            zero_pages_count++;
        }
    }

    effective_size -= zero_pages_count * PAGE_SIZE;
    *effective_size_out = effective_size;
    fseek(input, 0, SEEK_SET);

    unsigned char* buffer = malloc(buffsize);
    if (!buffer) {
        perror("Buffer allocation failed");
        fclose(input);
        return 0;
    }

    long total_compressed_size = 0;
    int chunk_num = 0;

    while ((bytes_read = fread(buffer, 1, buffsize, input)) > 0) {
        // Update file names per chunk
        snprintf(temp_file, sizeof(temp_file), "testdir/%s_chunk_%d.bin", filename, chunk_num);
        snprintf(compressed_file, sizeof(compressed_file), "testdir/%s_chunk_%d.zpaq", filename, chunk_num);

        FILE* temp = fopen(temp_file, "wb");
        if (!temp) {
            perror("Error creating temp chunk file");
            break;
        }

        fwrite(buffer, 1, bytes_read, temp);
        fclose(temp);

        char command[2048];
        snprintf(command, sizeof(command), "./masterzpaqd cinst 3 %s %s", compressed_file, temp_file);
        int ret = system(command);
        if (ret != 0) {
            printf("Compression failed for chunk %d: code %d\n", chunk_num, ret);
            remove(temp_file);
            continue;
        }

        struct stat st;
        if (stat(compressed_file, &st) == 0 && st.st_size > 0) {
            total_compressed_size += st.st_size;
        } else {
            printf("Compressed file missing or empty: %s\n", compressed_file);
        }

        remove(temp_file);
        remove(compressed_file);
        chunk_num++;
    }

    free(buffer);
    fclose(input);
    return total_compressed_size;
}


int main(int argc, char* argv[]) {
    if (argc != 3) {
        printf("Usage: ./ztester <dumpdirectory> <outputfile>\n");
        return 1;
    }

    int chunksize_map[] = {16384, 65536, 262144, 1048576, 2097152};

    char* dump_dir = argv[1];
    char* outpath = argv[2];

    glob_t glob_result;
    char directory_pattern[1024];
    snprintf(directory_pattern, sizeof(directory_pattern), "%s/*", dump_dir);

    if (glob(directory_pattern, 0, NULL, &glob_result) != 0) {
        perror("Error globbing files");
        return 1;
    }

    FILE* results = fopen(outpath, "a");
    if (!results) {
        perror("Error opening results file");
        globfree(&glob_result);
        return 1;
    }

    for (size_t file_idx = 0; file_idx < glob_result.gl_pathc; file_idx++) {
        const char* input_file = glob_result.gl_pathv[file_idx];
        printf("Processing file: %s\n", input_file);

        long regsize = 0;
        struct stat input_stat;
        if (stat(input_file, &input_stat) != 0) {
            perror("Error getting input file size");
            continue;
        }
        regsize = input_stat.st_size;

        for (int i = 0; i < 5; i++) {
            int chunksize = chunksize_map[i];
            
            long effective_size = 0;
            long compressed_size = compress_and_measure(input_file, chunksize, &effective_size);

            if (compressed_size > 0 && effective_size > 0) {
                float raw_cr = regsize / (float)compressed_size;
                float eff_cr = effective_size / (float)compressed_size;

                printf("%s buffsz %d, Raw CR, %.2f, Effective CR, %.2f\n",
                    input_file, chunksize, raw_cr, eff_cr);

                fprintf(results, "%s buffsz %d, Raw CR, %.2f, Effective CR, %.2f\n",
                    input_file, chunksize, raw_cr, eff_cr);
            } else {
                printf("%s buffsz %d: Compression failed or zero compressed size.\n",
                    input_file, chunksize);
                fprintf(results, "%s buffsz %d: Compression failed or zero compressed size.\n",
                    input_file, chunksize);
            }
        }

        fprintf(results, "\n");
        printf("\n");
    }

    fclose(results);
    globfree(&glob_result);

    return 0;
}
