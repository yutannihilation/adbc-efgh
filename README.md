# adbc-efgh

## Okay, I know [ADBC]. But, what does "efgh" stand for?

No idea. The last "h" is probably for "HTTP/3"? I'm still searching for cool words for "e", "f", and "g"!

[ADBC]: https://arrow.apache.org/adbc/current/index.html

## Update self-signed certificate

Use [cfssl](https://github.com/cloudflare/cfssl).

```sh
mkdir -p src/cert/
cd src/cert/
mkcert localhost 127.0.0.1 ::1

# convert to DER
openssl x509 -in ./localhost+2.pem -outform DER -out server.cert
openssl rsa -in ./localhost+2-key.pem -outform DER -out server.key
```
