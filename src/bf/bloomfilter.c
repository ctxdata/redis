#include "../redismodule.h"
#include <pthread.h>
#include <unistd.h>
#include <string.h>

static RedisModuleType *FunDBBloomFilterType;
#define MYTYPE_ENCODING_VERSION 0

uint hash(const char* str, size_t len) {
    uint h = 0;
    if (len > 0) {
        for (uint i = 0; i < len; i++) {
            h = 31 * h + str[i];
        }
    }
    return h;
}

/**
 * @brief 
 * 
 */
struct BloomFilter {
    size_t hashes;
    size_t size;
    char *ptr;
};

struct BloomFilter* createBloomFilter(size_t s, size_t h) {
    struct BloomFilter* p =  RedisModule_Alloc(sizeof(*p));
    p->size = s;
    p->hashes = h;
    p->ptr = RedisModule_Alloc(s);
    memset(p->ptr, 0, s);
    return p;
}

/**
 * @brief FunBF.add [key] [value]
 * 
 * @param ctx 
 * @param argv 
 * @param argc 
 * @return int : 1, if the value doesn't exist or 0, if the value is already existed.
 */
int FunBFAddCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    size_t len;
    uint i, slot, bit, h;
    char *c, mask;
    int exists = 1;
    int nokey = 0;
    struct BloomFilter *data;
    const char* str = RedisModule_StringPtrLen(argv[2], &len);

    RedisModuleKey *key;
    if (!RedisModule_KeyExists(ctx, argv[1])) {
        key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
        // To simplify this fake bloomfilter, use hard-coded bitmap & number of hash functions
        // There are 8K bits in total, and 2 hash functions to check existence
        data = createBloomFilter(1024, 2);
        nokey = 1;
    } else {
        key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
        int type = RedisModule_KeyType(key);

        if (type != REDISMODULE_KEYTYPE_EMPTY &&
            RedisModule_ModuleTypeGetType(key) != FunDBBloomFilterType) {
            return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        data = RedisModule_ModuleTypeGetValue(key);
    }

    h = hash(str, len);
    for (i=0; i<data->hashes; i++) {
        h = h*31 + 0xedb88320L;
        h = (h ^ ( h>>16 )) % (data->size << 3);
        slot = h >> 3;
        bit = h & 7;
        c = &(data->ptr[slot]);
        mask = 1<<bit;
        exists = exists && (*c & mask);
        *c |= mask;
    }

    if (nokey)
        RedisModule_ModuleTypeSetValue(key, FunDBBloomFilterType, data);

    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithBool(ctx, !exists);

    return REDISMODULE_OK;
}

/**
 * @brief FunBF.exists [key] [value]
 * 
 * @param ctx 
 * @param argv 
 * @param argc 
 * @return int 
 */
int FunBFExistsCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    struct BloomFilter *data;

    int exists = 1;
    size_t len;
    char c, mask;
    uint i, slot, bit, h;
    const char* str = RedisModule_StringPtrLen(argv[2], &len);
    if (!RedisModule_KeyExists(ctx, argv[1])) {
        RedisModule_ReplyWithError(ctx, "Key doesn't exist.");
        return REDISMODULE_ERR;
    } else {
        RedisModuleKey *key;
        key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
        int type = RedisModule_KeyType(key);

        if (type != REDISMODULE_KEYTYPE_EMPTY &&
            RedisModule_ModuleTypeGetType(key) != FunDBBloomFilterType) {
            return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        data = RedisModule_ModuleTypeGetValue(key);
    }

    h = hash(str, len);
    for (i=0; i<data->hashes; i++) {
        h = h*31 + 0xedb88320L;
        h = (h ^ ( h>>16 )) % (data->size << 3);
        slot = h >> 3;
        bit = h & 7;
        c = data->ptr[slot];
        mask = 1<<bit;
        exists = exists && (c & mask);
        if (!exists) {
            break;
        }
    }

    RedisModule_ReplyWithBool(ctx, exists);
    return REDISMODULE_OK;
}

/**
 * @brief Handle AOF Rewrite
 * RedisModule_EmitAOF(aof, "FunBF.load", "sllb", key, bf->hashes, bf->size, bf->ptr, bf->size);
 * 
 * @param ctx 
 * @param argv 
 * @param argc 
 * @return int 
 */
int FunBFLoadCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    struct BloomFilter *data;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    size_t s, h;

    RedisModule_StringToLongLong(argv[2], &h);
    RedisModule_StringToLongLong(argv[3], &s);
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        // SetValue
        data = createBloomFilter(s, h);
        RedisModule_ModuleTypeSetValue(key, FunDBBloomFilterType, data);
    } else {
        RedisModuleKey *key;
        key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
        int type = RedisModule_KeyType(key);

        if (type != REDISMODULE_KEYTYPE_EMPTY &&
            RedisModule_ModuleTypeGetType(key) != FunDBBloomFilterType) {
            return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        data = RedisModule_ModuleTypeGetValue(key);
        data->hashes = h;
        data->size = s;
        RedisModule_Free(data->ptr);
    }

    const char *buf = RedisModule_StringPtrLen(argv[4], &data->size);
    data->ptr = RedisModule_Alloc(data->size);
    memset(data->ptr, buf, data->size);

    return REDISMODULE_OK;
}

static void *BloomFilterRDBLoad(struct RedisModuleIO *io, int encver) {
    REDISMODULE_NOT_USED(encver);

    struct BloomFilter* p =  RedisModule_Alloc(sizeof(*p));
    p->hashes = RedisModule_LoadUnsigned(io);
    p->size = RedisModule_LoadUnsigned(io);
    p->ptr = RedisModule_LoadStringBuffer(io, NULL);

    return p;
}

static void BloomFilterRDBSave(struct RedisModuleIO *rdb, void *value) {
    struct BloomFilter* bf = value;
    RedisModule_SaveUnsigned(rdb, bf->hashes);
    RedisModule_SaveUnsigned(rdb, bf->size);
    RedisModule_SaveStringBuffer(rdb, bf->ptr, bf->size);
}

static void BloomFilterAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    struct BloomFilter* bf = value;

    RedisModule_EmitAOF(aof, "FunBF.load", "sllb", key, bf->hashes, bf->size, bf->ptr, bf->size);
}

static void BloomFilterTypeFree(void *value) {
    if (value) {
        struct BloomFilter* p = value;
        RedisModule_Free(p->ptr);
        RedisModule_Free(value);
    }
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "FunBF", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = 3,
        .rdb_load = BloomFilterRDBLoad,
        .rdb_save = BloomFilterRDBSave,
        .aof_rewrite = BloomFilterAofRewrite,
        .free = BloomFilterTypeFree
    };

    FunDBBloomFilterType = RedisModule_CreateDataType(ctx, "FunBFiltr", MYTYPE_ENCODING_VERSION, &tm);
    if (FunDBBloomFilterType == NULL)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"FunBF.add", FunBFAddCommand_RedisCommand,"",1,2,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"FunBF.exists", FunBFExistsCommand_RedisCommand,"",1,2,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    if (RedisModule_CreateCommand(ctx,"FunBF.load", FunBFLoadCommand_RedisCommand,"",1,2,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
