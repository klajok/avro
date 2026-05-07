# Memory Allocation Controls

## Summary

This library includes configuration options to prevent denial-of-service attacks through excessive memory allocation when processing untrusted Avro data. Users should configure appropriate limits based on their use case.

## Required Configuration

Users processing untrusted Avro data **must** configure appropriate size limits using the `Config` struct. Adjust these values based on your legitimate data requirements. Lower values provide better protection but may reject valid large datasets.

```go
cfg := avro.Config{
    MaxByteSliceSize:  102_400, // 100 kiB - limit for bytes/string types
    MaxSliceAllocSize: 10_000,  // 10k - limit for array lengths
    MaxMapAllocSize:   10_000,  // 10k - limit for map sizes
}
api := cfg.Freeze()

// Now use the frozen API for untrusted data, e.g.:
err := api.Unmarshal(schema, untrustedData, &result)
```

Alternatively update the defaults globally:

```go
avro.DefaultConfig = avro.Config{
    MaxByteSliceSize:  102_400, // 100 kiB - limit for bytes/string types
    MaxSliceAllocSize: 10_000,  // 10k - limit for array lengths
    MaxMapAllocSize:   10_000,  // 10k - limit for map sizes
}.Freeze()

// Now use the Avro functionality for untrusted data, e.g.:
decoder, err := avro.NewDecoder(schema, untrustedReader)
```

## Configuration Guidelines

1. **MaxByteSliceSize**: Controls the maximum size of individual `bytes` or `string` values. Default: 1 MiB. Set to -1 to disable (not recommended for untrusted input).

2. **MaxSliceAllocSize**: Controls the maximum length of an individual array. Default: unlimited. Set this to a reasonable value based on your expected data size.

3. **MaxMapAllocSize**: Controls the maximum capacity of an individual map. Default: unlimited. Set this to a reasonable value based on your expected data size.
