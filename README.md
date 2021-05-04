# asdurable
temporary repo to test durable writes on AS

```
GOOS=linux go build
scp asdurable gp15.stg.redsift.tech:/tmp/
ssh gp15.stg.redsift.tech /tmp/asdurable -n 10000 -durable
```

