#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <glob.h>
#include <string.h>

long compress_and_measure(const char* input_file, int buffsize) {
    FILE* input = fopen(input_file, "rb");
    if (!input) {
        perror("Error opening input file");
        return 0;
    }

    char temp_file[buffsize * 2];
    char compressed_file[buffsize * 2];

    const char* filename = strrchr(input_file, '/');
    filename = (filename) ? filename + 1 : input_file; // Move past '/' if found

    // Construct file paths
    snprintf(temp_file, sizeof(temp_file), "testdir/%s_temp_chunk_%d.bin", filename, buffsize);
    snprintf(compressed_file, sizeof(compressed_file), "testdir/%s_temp_chunk_%d.zpaq", filename, buffsize);

    printf("Temp file: %s\n", temp_file);
    printf("Compressed file: %s\n", compressed_file);

    // Get total file size
    fseek(input, 0, SEEK_END);
    long total_size = ftell(input);
    fseek(input, 0, SEEK_SET);

    // Check for empty file
    if (total_size == 0) {
        printf("Input file is empty: %s\n", input_file);
        fclose(input);
        return 0;
    }
    
    int total_pages = total_size / buffsize;
    unsigned char buffer[buffsize];
    size_t bytes_read;
    long total_compressed_size = 0;
    int page_number = 0;

    printf("\nBuffer size: %d bytes (%d total pages)\n", buffsize, total_pages);

    while ((bytes_read = fread(buffer, 1, buffsize, input)) > 0) {

        // Write chunk to temp file
        FILE* temp = fopen(temp_file, "wb");
        if (!temp) {
            perror("Error opening temp file");
            fclose(input);
            return total_compressed_size;
        }
        fwrite(buffer, 1, bytes_read, temp);
        fclose(temp);

        // Verify temp file exists
        if (access(temp_file, F_OK) == -1) {
            perror("Temp file was not created successfully");
            fclose(input);
            return total_compressed_size;
        }

        // Run zpaqd compression
        char command[2048];
        snprintf(command, sizeof(command), "./zpaqd c 3 %s %s", compressed_file, temp_file);
        int ret = system(command);
        if (ret != 0) {
            printf("zpaqd failed on page %d/%d with code: %d\n", page_number, total_pages, ret);
        }

        // Measure compressed file size
        struct stat st;
        if (stat(compressed_file, &st) == 0) {
            if (st.st_size == 0) {
                printf("Compressed file size is zero, compression may have failed: %s\n", compressed_file);
            } else {
                total_compressed_size += st.st_size;
            }
        } else {
            perror("Error getting compressed file size");
        }

        // Clean up files
        remove(compressed_file);
        remove(temp_file);

        // Print page progress
        printf("Processed page %d / %d\n", page_number, total_pages);
        printf("\nCSIZE: %ld\n", total_compressed_size);
        page_number++;
    }

    fclose(input);
    printf("Aggregate compressed size for buffer %d: %ld bytes\n", buffsize, total_compressed_size);
    return total_compressed_size;
}

int main(int argc, char* argv[]) {

    // usage ./ztester chunksize dumpdirectory outputfile
    // 16384, 65536, 262144, 1048576
    int chunksize = atoi(argv[1]);

    if (chunksize == 0) {
        chunksize = 16384;
    } else if (chunksize == 1) {
        chunksize = 65536;
    } else if (chunksize == 2) {
        chunksize = 262144;
    } else if (chunksize == 3){
        chunksize = 1048576;
    } else {
        printf("incorrect chunksize\n");
        return 1;
    }

    glob_t glob_result;
    char directory_pattern[1024];  // Buffer to hold the full pattern
    snprintf(directory_pattern, sizeof(directory_pattern), "%s/*", argv[2]);

    char* outpath = argv[3];

    // Glob to find files in directory
    if (glob(directory_pattern, 0, NULL, &glob_result) != 0) {
        perror("Error globbing files");
        return 1;
    }

    FILE* results = fopen(outpath, "a");  // Open file to save results
    if (!results) {
        perror("Error opening results file");
        globfree(&glob_result);  // Clean up glob
        return 1;
    }

    for (size_t i = 0; i < glob_result.gl_pathc; i++) {
        const char* input_file = glob_result.gl_pathv[i];
        printf("Processing file: %s\n", input_file);
    
        // Get input file size for compression ratio
        struct stat input_stat;
        if (stat(input_file, &input_stat) != 0) {
            perror("Error getting input file size");
            continue;
        }
        float regsize = input_stat.st_size;
    
        long compressed_size = compress_and_measure(input_file, chunksize);
    
        if (compressed_size > 0) {
            printf("\n%s buffsz %d: %f CR\n", input_file, chunksize, regsize / (float)compressed_size);
            fprintf(results, "%s buffsz %d: %f CR\n", input_file, chunksize, regsize / (float)compressed_size);
        } else {
            printf("\n%s: Compression failed or zero compressed size.\n", input_file);
            fprintf(results, "%s: Compression failed or zero compressed size.\n", input_file);
        }
    }
    

    fclose(results);  // Close file
    globfree(&glob_result);  // Clean up glob

    return 0;
}
