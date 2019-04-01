package encrypt

import (
	"context"

	jose "gopkg.in/square/go-jose.v2"
)

// JWE is a wrapper around the jose jwe encryption function.
func JWE(ctx context.Context, key jose.JSONWebKey, plaintext []byte) (*jose.JSONWebEncryption, error) {

	rec := jose.Recipient{
		Algorithm: jose.DIRECT, // jose.A256GCMKW // direct or key wrapping
		Key:       key.Key,
	}

	alg := jose.A256GCM
	encrypter, err := jose.NewEncrypter(alg, rec, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Encrypt or EncryptWithAuthData? What is the auth data?
	jwe, err := encrypter.Encrypt(plaintext)
	if err != nil {
		return nil, err
	}

	return jwe, nil
}
