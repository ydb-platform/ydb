/* GIO - GLib Input, Output and Certificateing Library
 *
 * Copyright (C) 2010 Red Hat, Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include "gtlscertificate.h"

#include <string.h>
#include "ginitable.h"
#include "gtlsbackend.h"
#include "gtlsconnection.h"
#include "glibintl.h"

/**
 * SECTION:gtlscertificate
 * @title: GTlsCertificate
 * @short_description: TLS certificate
 * @include: gio/gio.h
 * @see_also: #GTlsConnection
 *
 * A certificate used for TLS authentication and encryption.
 * This can represent either a certificate only (eg, the certificate
 * received by a client from a server), or the combination of
 * a certificate and a private key (which is needed when acting as a
 * #GTlsServerConnection).
 *
 * Since: 2.28
 */

/**
 * GTlsCertificate:
 *
 * Abstract base class for TLS certificate types.
 *
 * Since: 2.28
 */

struct _GTlsCertificatePrivate {
  gboolean pkcs12_properties_not_overridden;
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE (GTlsCertificate, g_tls_certificate, G_TYPE_OBJECT)

enum
{
  PROP_0,

  PROP_CERTIFICATE,
  PROP_CERTIFICATE_PEM,
  PROP_PRIVATE_KEY,
  PROP_PRIVATE_KEY_PEM,
  PROP_ISSUER,
  PROP_PKCS11_URI,
  PROP_PRIVATE_KEY_PKCS11_URI,
  PROP_NOT_VALID_BEFORE,
  PROP_NOT_VALID_AFTER,
  PROP_SUBJECT_NAME,
  PROP_ISSUER_NAME,
  PROP_DNS_NAMES,
  PROP_IP_ADDRESSES,
  PROP_PKCS12_DATA,
  PROP_PASSWORD,
};

static void
g_tls_certificate_init (GTlsCertificate *cert)
{
}

static void
g_tls_certificate_get_property (GObject    *object,
				guint       prop_id,
				GValue     *value,
				GParamSpec *pspec)
{
  switch (prop_id)
    {
    /* Subclasses must override these properties but this allows older backends to not fatally error */
    case PROP_PRIVATE_KEY:
    case PROP_PRIVATE_KEY_PEM:
    case PROP_PKCS11_URI:
    case PROP_PRIVATE_KEY_PKCS11_URI:
      g_value_set_static_string (value, NULL);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_tls_certificate_set_property (GObject      *object,
				guint         prop_id,
				const GValue *value,
				GParamSpec   *pspec)
{
  GTlsCertificate *cert = (GTlsCertificate*)object;
  GTlsCertificatePrivate *priv = g_tls_certificate_get_instance_private (cert);

  switch (prop_id)
    {
    case PROP_PKCS11_URI:
    case PROP_PRIVATE_KEY_PKCS11_URI:
      /* Subclasses must override these properties but this allows older backends to not fatally error. */
      break;
    case PROP_PKCS12_DATA:
    case PROP_PASSWORD:
      /* We don't error on setting these properties however we track that they were not overridden. */
      priv->pkcs12_properties_not_overridden = TRUE;
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_tls_certificate_class_init (GTlsCertificateClass *class)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (class);

  gobject_class->set_property = g_tls_certificate_set_property;
  gobject_class->get_property = g_tls_certificate_get_property;

  /**
   * GTlsCertificate:pkcs12-data: (nullable)
   *
   * The PKCS #12 formatted data used to construct the object.
   *
   * See also: g_tls_certificate_new_from_pkcs12()
   *
   * Since: 2.72
   */
  g_object_class_install_property (gobject_class, PROP_PKCS12_DATA,
				   g_param_spec_boxed ("pkcs12-data",
						       P_("PKCS #12 data"),
						       P_("The PKCS #12 data used for construction"),
						       G_TYPE_BYTE_ARRAY,
						       G_PARAM_WRITABLE |
						       G_PARAM_CONSTRUCT_ONLY |
						       G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:password: (nullable)
   *
   * An optional password used when constructed with GTlsCertificate:pkcs12-data.
   *
   * Since: 2.72
   */
  g_object_class_install_property (gobject_class, PROP_PASSWORD,
                                   g_param_spec_string ("password",
                                                        P_("Password"),
                                                        P_("Password used when constructing from bytes"),
                                                        NULL,
                                                        G_PARAM_WRITABLE |
                                                          G_PARAM_CONSTRUCT_ONLY |
                                                          G_PARAM_STATIC_STRINGS));
  /**
   * GTlsCertificate:certificate:
   *
   * The DER (binary) encoded representation of the certificate.
   * This property and the #GTlsCertificate:certificate-pem property
   * represent the same data, just in different forms.
   *
   * Since: 2.28
   */
  g_object_class_install_property (gobject_class, PROP_CERTIFICATE,
				   g_param_spec_boxed ("certificate",
						       P_("Certificate"),
						       P_("The DER representation of the certificate"),
						       G_TYPE_BYTE_ARRAY,
						       G_PARAM_READWRITE |
						       G_PARAM_CONSTRUCT_ONLY |
						       G_PARAM_STATIC_STRINGS));
  /**
   * GTlsCertificate:certificate-pem:
   *
   * The PEM (ASCII) encoded representation of the certificate.
   * This property and the #GTlsCertificate:certificate
   * property represent the same data, just in different forms.
   *
   * Since: 2.28
   */
  g_object_class_install_property (gobject_class, PROP_CERTIFICATE_PEM,
				   g_param_spec_string ("certificate-pem",
							P_("Certificate (PEM)"),
							P_("The PEM representation of the certificate"),
							NULL,
							G_PARAM_READWRITE |
							G_PARAM_CONSTRUCT_ONLY |
							G_PARAM_STATIC_STRINGS));
  /**
   * GTlsCertificate:private-key: (nullable)
   *
   * The DER (binary) encoded representation of the certificate's
   * private key, in either [PKCS \#1 format](https://datatracker.ietf.org/doc/html/rfc8017)
   * or unencrypted [PKCS \#8 format.](https://datatracker.ietf.org/doc/html/rfc5208)
   * PKCS \#8 format is supported since 2.32; earlier releases only
   * support PKCS \#1. You can use the `openssl rsa` tool to convert
   * PKCS \#8 keys to PKCS \#1.
   *
   * This property (or the #GTlsCertificate:private-key-pem property)
   * can be set when constructing a key (for example, from a file).
   * Since GLib 2.70, it is now also readable; however, be aware that if
   * the private key is backed by a PKCS \#11 URI – for example, if it
   * is stored on a smartcard – then this property will be %NULL. If so,
   * the private key must be referenced via its PKCS \#11 URI,
   * #GTlsCertificate:private-key-pkcs11-uri. You must check both
   * properties to see if the certificate really has a private key.
   * When this property is read, the output format will be unencrypted
   * PKCS \#8.
   *
   * Since: 2.28
   */
  g_object_class_install_property (gobject_class, PROP_PRIVATE_KEY,
				   g_param_spec_boxed ("private-key",
						       P_("Private key"),
						       P_("The DER representation of the certificate’s private key"),
						       G_TYPE_BYTE_ARRAY,
						       G_PARAM_READWRITE |
						       G_PARAM_CONSTRUCT_ONLY |
						       G_PARAM_STATIC_STRINGS));
  /**
   * GTlsCertificate:private-key-pem: (nullable)
   *
   * The PEM (ASCII) encoded representation of the certificate's
   * private key in either [PKCS \#1 format](https://datatracker.ietf.org/doc/html/rfc8017)
   * ("`BEGIN RSA PRIVATE KEY`") or unencrypted
   * [PKCS \#8 format](https://datatracker.ietf.org/doc/html/rfc5208)
   * ("`BEGIN PRIVATE KEY`"). PKCS \#8 format is supported since 2.32;
   * earlier releases only support PKCS \#1. You can use the `openssl rsa`
   * tool to convert PKCS \#8 keys to PKCS \#1.
   *
   * This property (or the #GTlsCertificate:private-key property)
   * can be set when constructing a key (for example, from a file).
   * Since GLib 2.70, it is now also readable; however, be aware that if
   * the private key is backed by a PKCS \#11 URI - for example, if it
   * is stored on a smartcard - then this property will be %NULL. If so,
   * the private key must be referenced via its PKCS \#11 URI,
   * #GTlsCertificate:private-key-pkcs11-uri. You must check both
   * properties to see if the certificate really has a private key.
   * When this property is read, the output format will be unencrypted
   * PKCS \#8.
   *
   * Since: 2.28
   */
  g_object_class_install_property (gobject_class, PROP_PRIVATE_KEY_PEM,
				   g_param_spec_string ("private-key-pem",
							P_("Private key (PEM)"),
							P_("The PEM representation of the certificate’s private key"),
							NULL,
							G_PARAM_READWRITE |
							G_PARAM_CONSTRUCT_ONLY |
							G_PARAM_STATIC_STRINGS));
  /**
   * GTlsCertificate:issuer:
   *
   * A #GTlsCertificate representing the entity that issued this
   * certificate. If %NULL, this means that the certificate is either
   * self-signed, or else the certificate of the issuer is not
   * available.
   *
   * Beware the issuer certificate may not be the same as the
   * certificate that would actually be used to construct a valid
   * certification path during certificate verification.
   * [RFC 4158](https://datatracker.ietf.org/doc/html/rfc4158) explains
   * why an issuer certificate cannot be naively assumed to be part of the
   * the certification path (though GLib's TLS backends may not follow the
   * path building strategies outlined in this RFC). Due to the complexity
   * of certification path building, GLib does not provide any way to know
   * which certification path will actually be used. Accordingly, this
   * property cannot be used to make security-related decisions. Only
   * GLib itself should make security decisions about TLS certificates.
   *
   * Since: 2.28
   */
  g_object_class_install_property (gobject_class, PROP_ISSUER,
				   g_param_spec_object ("issuer",
							P_("Issuer"),
							P_("The certificate for the issuing entity"),
							G_TYPE_TLS_CERTIFICATE,
							G_PARAM_READWRITE |
							G_PARAM_CONSTRUCT_ONLY |
							G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:pkcs11-uri: (nullable)
   *
   * A URI referencing the [PKCS \#11](https://docs.oasis-open.org/pkcs11/pkcs11-base/v3.0/os/pkcs11-base-v3.0-os.html)
   * objects containing an X.509 certificate and optionally a private key.
   *
   * If %NULL, the certificate is either not backed by PKCS \#11 or the
   * #GTlsBackend does not support PKCS \#11.
   *
   * Since: 2.68
   */
  g_object_class_install_property (gobject_class, PROP_PKCS11_URI,
                                   g_param_spec_string ("pkcs11-uri",
                                                        P_("PKCS #11 URI"),
                                                        P_("The PKCS #11 URI"),
                                                        NULL,
                                                        G_PARAM_READWRITE |
                                                          G_PARAM_CONSTRUCT_ONLY |
                                                          G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:private-key-pkcs11-uri: (nullable)
   *
   * A URI referencing a [PKCS \#11](https://docs.oasis-open.org/pkcs11/pkcs11-base/v3.0/os/pkcs11-base-v3.0-os.html)
   * object containing a private key.
   *
   * Since: 2.68
   */
  g_object_class_install_property (gobject_class, PROP_PRIVATE_KEY_PKCS11_URI,
                                   g_param_spec_string ("private-key-pkcs11-uri",
                                                        P_("PKCS #11 URI"),
                                                        P_("The PKCS #11 URI for a private key"),
                                                        NULL,
                                                        G_PARAM_READWRITE |
                                                          G_PARAM_CONSTRUCT_ONLY |
                                                          G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:not-valid-before: (nullable)
   *
   * The time at which this cert is considered to be valid,
   * %NULL if unavailable.
   *
   * Since: 2.70
   */
  g_object_class_install_property (gobject_class, PROP_NOT_VALID_BEFORE,
                                   g_param_spec_boxed ("not-valid-before",
                                                       P_("Not Valid Before"),
                                                       P_("Cert should not be considered valid before this time."),
                                                       G_TYPE_DATE_TIME,
                                                       G_PARAM_READABLE |
                                                         G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:not-valid-after: (nullable)
   *
   * The time at which this cert is no longer valid,
   * %NULL if unavailable.
   *
   * Since: 2.70
   */
  g_object_class_install_property (gobject_class, PROP_NOT_VALID_AFTER,
                                   g_param_spec_boxed ("not-valid-after",
                                                       P_("Not Valid after"),
                                                       P_("Cert should not be considered valid after this time."),
                                                       G_TYPE_DATE_TIME,
                                                       G_PARAM_READABLE |
                                                         G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:subject-name: (nullable)
   *
   * The subject from the cert,
   * %NULL if unavailable.
   *
   * Since: 2.70
   */
  g_object_class_install_property (gobject_class, PROP_SUBJECT_NAME,
                                   g_param_spec_string ("subject-name",
                                                        P_("Subject Name"),
                                                        P_("The subject name from the certificate."),
                                                        NULL,
                                                        G_PARAM_READABLE |
                                                          G_PARAM_STATIC_STRINGS));
  /**
   * GTlsCertificate:issuer-name: (nullable)
   *
   * The issuer from the certificate,
   * %NULL if unavailable.
   *
   * Since: 2.70
   */
  g_object_class_install_property (gobject_class, PROP_ISSUER_NAME,
                                   g_param_spec_string ("issuer-name",
                                                        P_("Issuer Name"),
                                                        P_("The issuer from the certificate."),
                                                        NULL,
                                                        G_PARAM_READABLE |
                                                          G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:dns-names: (nullable) (element-type GBytes) (transfer container)
   *
   * The DNS names from the certificate's Subject Alternative Names (SANs),
   * %NULL if unavailable.
   *
   * Since: 2.70
   */
  g_object_class_install_property (gobject_class, PROP_DNS_NAMES,
                                   g_param_spec_boxed ("dns-names",
                                                       P_("DNS Names"),
                                                       P_("DNS Names listed on the cert."),
                                                       G_TYPE_PTR_ARRAY,
                                                       G_PARAM_READABLE |
                                                         G_PARAM_STATIC_STRINGS));

  /**
   * GTlsCertificate:ip-addresses: (nullable) (element-type GInetAddress) (transfer container)
   *
   * The IP addresses from the certificate's Subject Alternative Names (SANs),
   * %NULL if unavailable.
   *
   * Since: 2.70
   */
  g_object_class_install_property (gobject_class, PROP_IP_ADDRESSES,
                                   g_param_spec_boxed ("ip-addresses",
                                                       P_("IP Addresses"),
                                                       P_("IP Addresses listed on the cert."),
                                                       G_TYPE_PTR_ARRAY,
                                                       G_PARAM_READABLE |
                                                         G_PARAM_STATIC_STRINGS));
}

static GTlsCertificate *
g_tls_certificate_new_internal (const gchar      *certificate_pem,
				const gchar      *private_key_pem,
				GTlsCertificate  *issuer,
				GError          **error)
{
  GObject *cert;
  GTlsBackend *backend;

  backend = g_tls_backend_get_default ();

  cert = g_initable_new (g_tls_backend_get_certificate_type (backend),
			 NULL, error,
			 "certificate-pem", certificate_pem,
			 "private-key-pem", private_key_pem,
			 "issuer", issuer,
			 NULL);

  return G_TLS_CERTIFICATE (cert);
}

#define PEM_CERTIFICATE_HEADER     "-----BEGIN CERTIFICATE-----"
#define PEM_CERTIFICATE_FOOTER     "-----END CERTIFICATE-----"
#define PEM_PRIVKEY_HEADER_BEGIN   "-----BEGIN "
#define PEM_PRIVKEY_HEADER_END     "PRIVATE KEY-----"
#define PEM_PRIVKEY_FOOTER_BEGIN   "-----END "
#define PEM_PRIVKEY_FOOTER_END     "PRIVATE KEY-----"
#define PEM_PKCS8_ENCRYPTED_HEADER "-----BEGIN ENCRYPTED PRIVATE KEY-----"

static gchar *
parse_private_key (const gchar *data,
		   gsize data_len,
		   gboolean required,
		   GError **error)
{
  const gchar *header_start = NULL, *header_end, *footer_start = NULL, *footer_end;
  const gchar *data_end = data + data_len;

  header_end = g_strstr_len (data, data_len, PEM_PRIVKEY_HEADER_END);
  if (header_end)
    header_start = g_strrstr_len (data, header_end - data, PEM_PRIVKEY_HEADER_BEGIN);

  if (!header_start)
    {
      if (required)
	g_set_error_literal (error, G_TLS_ERROR, G_TLS_ERROR_BAD_CERTIFICATE,
			     _("No PEM-encoded private key found"));

      return NULL;
    }

  header_end += strlen (PEM_PRIVKEY_HEADER_END);

  if (strncmp (header_start, PEM_PKCS8_ENCRYPTED_HEADER, header_end - header_start) == 0)
    {
      g_set_error_literal (error, G_TLS_ERROR, G_TLS_ERROR_BAD_CERTIFICATE,
			   _("Cannot decrypt PEM-encoded private key"));
      return NULL;
    }

  footer_end = g_strstr_len (header_end, data_len - (header_end - data), PEM_PRIVKEY_FOOTER_END);
  if (footer_end)
    footer_start = g_strrstr_len (header_end, footer_end - header_end, PEM_PRIVKEY_FOOTER_BEGIN);

  if (!footer_start)
    {
      g_set_error_literal (error, G_TLS_ERROR, G_TLS_ERROR_BAD_CERTIFICATE,
			   _("Could not parse PEM-encoded private key"));
      return NULL;
    }

  footer_end += strlen (PEM_PRIVKEY_FOOTER_END);

  while ((footer_end < data_end) && (*footer_end == '\r' || *footer_end == '\n'))
    footer_end++;

  return g_strndup (header_start, footer_end - header_start);
}


static gchar *
parse_next_pem_certificate (const gchar **data,
			    const gchar  *data_end,
			    gboolean      required,
			    GError      **error)
{
  const gchar *start, *end;

  start = g_strstr_len (*data, data_end - *data, PEM_CERTIFICATE_HEADER);
  if (!start)
    {
      if (required)
	{
	  g_set_error_literal (error, G_TLS_ERROR, G_TLS_ERROR_BAD_CERTIFICATE,
			       _("No PEM-encoded certificate found"));
	}
      return NULL;
    }

  end = g_strstr_len (start, data_end - start, PEM_CERTIFICATE_FOOTER);
  if (!end)
    {
      g_set_error_literal (error, G_TLS_ERROR, G_TLS_ERROR_BAD_CERTIFICATE,
			   _("Could not parse PEM-encoded certificate"));
      return NULL;
    }
  end += strlen (PEM_CERTIFICATE_FOOTER);
  while ((end < data_end) && (*end == '\r' || *end == '\n'))
    end++;

  *data = end;

  return g_strndup (start, end - start);
}

static GSList *
parse_and_create_certificate_list (const gchar  *data,
                                   gsize         data_len,
                                   GError      **error)
{
  GSList *first_pem_list = NULL, *pem_list = NULL;
  gchar *first_pem;
  const gchar *p, *end;

  p = data;
  end = p + data_len;

  /* Make sure we can load, at least, one certificate. */
  first_pem = parse_next_pem_certificate (&p, end, TRUE, error);
  if (!first_pem)
    return NULL;

  /* Create a list with a single element. If we load more certificates
   * below, we will concatenate the two lists at the end. */
  first_pem_list = g_slist_prepend (first_pem_list, first_pem);

  /* If we read one certificate successfully, let's see if we can read
   * some more. If not, we will simply return a list with the first one.
   */
  while (p < end && p && *p)
    {
      gchar *cert_pem;
      GError *error = NULL;

      cert_pem = parse_next_pem_certificate (&p, end, FALSE, &error);
      if (error)
        {
          g_slist_free_full (pem_list, g_free);
          g_error_free (error);
          return first_pem_list;
        }
      else if (!cert_pem)
        {
          break;
        }

      pem_list = g_slist_prepend (pem_list, cert_pem);
    }

  pem_list = g_slist_concat (pem_list, first_pem_list);

  return pem_list;
}

static GTlsCertificate *
create_certificate_chain_from_list (GSList       *pem_list,
                                    const gchar  *key_pem)
{
  GTlsCertificate *cert = NULL, *issuer = NULL, *root = NULL;
  GTlsCertificateFlags flags;
  GSList *pem;

  pem = pem_list;
  while (pem)
    {
      const gchar *key = NULL;

      /* Private key belongs only to the first certificate. */
      if (!pem->next)
        key = key_pem;

      /* We assume that the whole file is a certificate chain, so we use
       * each certificate as the issuer of the next one (list is in
       * reverse order).
       */
      issuer = cert;
      cert = g_tls_certificate_new_internal (pem->data, key, issuer, NULL);
      if (issuer)
        g_object_unref (issuer);

      if (!cert)
        return NULL;

      /* root will point to the last certificate in the file. */
      if (!root)
        root = cert;

      pem = g_slist_next (pem);
    }

  /* Verify that the certificates form a chain. (We don't care at this
   * point if there are other problems with it.)
   */
  flags = g_tls_certificate_verify (cert, NULL, root);
  if (flags & G_TLS_CERTIFICATE_UNKNOWN_CA)
    {
      /* It wasn't a chain, it's just a bunch of unrelated certs. */
      g_clear_object (&cert);
    }

  return cert;
}

static GTlsCertificate *
parse_and_create_certificate (const gchar  *data,
                              gsize         data_len,
                              const gchar  *key_pem,
                              GError      **error)

{
  GSList *pem_list;
  GTlsCertificate *cert;

  pem_list = parse_and_create_certificate_list (data, data_len, error);
  if (!pem_list)
    return NULL;

  /* We don't pass the error here because, if it fails, we still want to
   * load and return the first certificate.
   */
  cert = create_certificate_chain_from_list (pem_list, key_pem);
  if (!cert)
    {
      GSList *last = NULL;

      /* Get the first certificate (which is the last one as the list is
       * in reverse order).
       */
      last = g_slist_last (pem_list);

      cert = g_tls_certificate_new_internal (last->data, key_pem, NULL, error);
    }

  g_slist_free_full (pem_list, g_free);

  return cert;
}

/**
 * g_tls_certificate_new_from_pem:
 * @data: PEM-encoded certificate data
 * @length: the length of @data, or -1 if it's 0-terminated.
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Creates a #GTlsCertificate from the PEM-encoded data in @data. If
 * @data includes both a certificate and a private key, then the
 * returned certificate will include the private key data as well. (See
 * the #GTlsCertificate:private-key-pem property for information about
 * supported formats.)
 *
 * The returned certificate will be the first certificate found in
 * @data. As of GLib 2.44, if @data contains more certificates it will
 * try to load a certificate chain. All certificates will be verified in
 * the order found (top-level certificate should be the last one in the
 * file) and the #GTlsCertificate:issuer property of each certificate
 * will be set accordingly if the verification succeeds. If any
 * certificate in the chain cannot be verified, the first certificate in
 * the file will still be returned.
 *
 * Returns: the new certificate, or %NULL if @data is invalid
 *
 * Since: 2.28
 */
GTlsCertificate *
g_tls_certificate_new_from_pem  (const gchar  *data,
				 gssize        length,
				 GError      **error)
{
  GError *child_error = NULL;
  gchar *key_pem;
  GTlsCertificate *cert;

  g_return_val_if_fail (data != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (length == -1)
    length = strlen (data);

  key_pem = parse_private_key (data, length, FALSE, &child_error);
  if (child_error != NULL)
    {
      g_propagate_error (error, child_error);
      return NULL;
    }

  cert = parse_and_create_certificate (data, length, key_pem, error);
  g_free (key_pem);

  return cert;
}

/**
 * g_tls_certificate_new_from_pkcs12:
 * @data: (array length=length): DER-encoded PKCS #12 format certificate data
 * @length: the length of @data
 * @password: (nullable): optional password for encrypted certificate data
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Creates a #GTlsCertificate from the data in @data. It must contain
 * a certificate and matching private key.
 *
 * If extra certificates are included they will be verified as a chain
 * and the #GTlsCertificate:issuer property will be set.
 * All other data will be ignored.
 *
 * You can pass as single password for all of the data which will be
 * used both for the PKCS #12 container as well as encrypted
 * private keys. If decryption fails it will error with
 * %G_TLS_ERROR_BAD_CERTIFICATE_PASSWORD.
 *
 * This constructor requires support in the current #GTlsBackend.
 * If support is missing it will error with
 * %G_IO_ERROR_NOT_SUPPORTED.
 *
 * Other parsing failures will error with %G_TLS_ERROR_BAD_CERTIFICATE.
 *
 * Returns: the new certificate, or %NULL if @data is invalid
 *
 * Since: 2.72
 */
GTlsCertificate *
g_tls_certificate_new_from_pkcs12 (const guint8  *data,
                                   gsize          length,
                                   const gchar   *password,
                                   GError       **error)
{
  GObject *cert;
  GTlsBackend *backend;
  GByteArray *bytes;

  g_return_val_if_fail (data != NULL || length == 0, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  backend = g_tls_backend_get_default ();

  bytes = g_byte_array_new ();
  g_byte_array_append (bytes, data, length);

  cert = g_initable_new (g_tls_backend_get_certificate_type (backend),
                         NULL, error,
                         "pkcs12-data", bytes,
                         "password", password,
                         NULL);

  g_byte_array_unref (bytes);

  if (cert)
    {
      GTlsCertificatePrivate *priv = g_tls_certificate_get_instance_private (G_TLS_CERTIFICATE (cert));

      if (priv->pkcs12_properties_not_overridden)
        {
          g_clear_object (&cert);
          g_set_error_literal (error, G_IO_ERROR, G_IO_ERROR_NOT_SUPPORTED,
                               _("The current TLS backend does not support PKCS #12"));
          return NULL;
        }
    }

  return G_TLS_CERTIFICATE (cert);
}

/**
 * g_tls_certificate_new_from_file_with_password:
 * @file: (type filename): file containing a certificate to import
 * @password: (not nullable): password for PKCS #12 files
 * @error: #GError for error reporting, or %NULL to ignore
 *
 * Creates a #GTlsCertificate from the data in @file.
 *
 * If @file cannot be read or parsed, the function will return %NULL and
 * set @error.
 *
 * Any unknown file types will error with %G_IO_ERROR_NOT_SUPPORTED.
 * Currently only `.p12` and `.pfx` files are supported.
 * See g_tls_certificate_new_from_pkcs12() for more details.
 *
 * Returns: the new certificate, or %NULL on error
 *
 * Since: 2.72
 */
GTlsCertificate *
g_tls_certificate_new_from_file_with_password (const gchar  *file,
                                               const gchar  *password,
                                               GError      **error)
{
  GTlsCertificate *cert;
  gchar *contents;
  gsize length;

  g_return_val_if_fail (file != NULL, NULL);
  g_return_val_if_fail (password != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (!g_str_has_suffix (file, ".p12") && !g_str_has_suffix (file, ".pfx"))
    {
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_NOT_SUPPORTED,
                   "The file type of \"%s\" is unknown. Only .p12 and .pfx files are supported currently.", file);
      return NULL;
    }

  if (!g_file_get_contents (file, &contents, &length, error))
    return NULL;

  cert = g_tls_certificate_new_from_pkcs12 ((guint8 *)contents, length, password, error);

  g_free (contents);
  return cert;
}

/**
 * g_tls_certificate_new_from_file:
 * @file: (type filename): file containing a certificate to import
 * @error: #GError for error reporting, or %NULL to ignore
 *
 * Creates a #GTlsCertificate from the data in @file.
 *
 * As of 2.72, if the filename ends in `.p12` or `.pfx` the data is loaded by
 * g_tls_certificate_new_from_pkcs12() otherwise it is loaded by
 * g_tls_certificate_new_from_pem(). See those functions for
 * exact details.
 *
 * If @file cannot be read or parsed, the function will return %NULL and
 * set @error.
 *
 * Returns: the new certificate, or %NULL on error
 *
 * Since: 2.28
 */
GTlsCertificate *
g_tls_certificate_new_from_file (const gchar  *file,
                                 GError      **error)
{
  GTlsCertificate *cert;
  gchar *contents;
  gsize length;

  g_return_val_if_fail (file != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (!g_file_get_contents (file, &contents, &length, error))
    return NULL;

  if (g_str_has_suffix (file, ".p12") || g_str_has_suffix (file, ".pfx"))
    cert = g_tls_certificate_new_from_pkcs12 ((guint8 *)contents, length, NULL, error);
  else
    cert = g_tls_certificate_new_from_pem (contents, length, error);

  g_free (contents);
  return cert;
}

/**
 * g_tls_certificate_new_from_files:
 * @cert_file: (type filename): file containing one or more PEM-encoded
 *     certificates to import
 * @key_file: (type filename): file containing a PEM-encoded private key
 *     to import
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Creates a #GTlsCertificate from the PEM-encoded data in @cert_file
 * and @key_file. The returned certificate will be the first certificate
 * found in @cert_file. As of GLib 2.44, if @cert_file contains more
 * certificates it will try to load a certificate chain. All
 * certificates will be verified in the order found (top-level
 * certificate should be the last one in the file) and the
 * #GTlsCertificate:issuer property of each certificate will be set
 * accordingly if the verification succeeds. If any certificate in the
 * chain cannot be verified, the first certificate in the file will
 * still be returned.
 *
 * If either file cannot be read or parsed, the function will return
 * %NULL and set @error. Otherwise, this behaves like
 * g_tls_certificate_new_from_pem().
 *
 * Returns: the new certificate, or %NULL on error
 *
 * Since: 2.28
 */
GTlsCertificate *
g_tls_certificate_new_from_files (const gchar  *cert_file,
                                  const gchar  *key_file,
                                  GError      **error)
{
  GTlsCertificate *cert;
  gchar *cert_data, *key_data;
  gsize cert_len, key_len;
  gchar *key_pem;

  if (!g_file_get_contents (key_file, &key_data, &key_len, error))
    return NULL;

  key_pem = parse_private_key (key_data, key_len, TRUE, error);
  g_free (key_data);
  if (!key_pem)
    return NULL;

  if (!g_file_get_contents (cert_file, &cert_data, &cert_len, error))
    {
      g_free (key_pem);
      return NULL;
    }

  cert = parse_and_create_certificate (cert_data, cert_len, key_pem, error);
  g_free (cert_data);
  g_free (key_pem);
  return cert;
}

/**
 * g_tls_certificate_new_from_pkcs11_uris:
 * @pkcs11_uri: A PKCS \#11 URI
 * @private_key_pkcs11_uri: (nullable): A PKCS \#11 URI
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Creates a #GTlsCertificate from a
 * [PKCS \#11](https://docs.oasis-open.org/pkcs11/pkcs11-base/v3.0/os/pkcs11-base-v3.0-os.html) URI.
 *
 * An example @pkcs11_uri would be `pkcs11:model=Model;manufacturer=Manufacture;serial=1;token=My%20Client%20Certificate;id=%01`
 *
 * Where the token’s layout is:
 *
 * |[
 * Object 0:
 *   URL: pkcs11:model=Model;manufacturer=Manufacture;serial=1;token=My%20Client%20Certificate;id=%01;object=private%20key;type=private
 *   Type: Private key (RSA-2048)
 *   ID: 01
 *
 * Object 1:
 *   URL: pkcs11:model=Model;manufacturer=Manufacture;serial=1;token=My%20Client%20Certificate;id=%01;object=Certificate%20for%20Authentication;type=cert
 *   Type: X.509 Certificate (RSA-2048)
 *   ID: 01
 * ]|
 *
 * In this case the certificate and private key would both be detected and used as expected.
 * @pkcs_uri may also just reference an X.509 certificate object and then optionally
 * @private_key_pkcs11_uri allows using a private key exposed under a different URI.
 *
 * Note that the private key is not accessed until usage and may fail or require a PIN later.
 *
 * Returns: (transfer full): the new certificate, or %NULL on error
 *
 * Since: 2.68
 */
GTlsCertificate *
g_tls_certificate_new_from_pkcs11_uris (const gchar  *pkcs11_uri,
                                        const gchar  *private_key_pkcs11_uri,
                                        GError      **error)
{
  GObject *cert;
  GTlsBackend *backend;

  g_return_val_if_fail (error == NULL || *error == NULL, NULL);
  g_return_val_if_fail (pkcs11_uri, NULL);

  backend = g_tls_backend_get_default ();

  cert = g_initable_new (g_tls_backend_get_certificate_type (backend),
                         NULL, error,
                         "pkcs11-uri", pkcs11_uri,
                         "private-key-pkcs11-uri", private_key_pkcs11_uri,
                         NULL);

  if (cert != NULL)
    {
      gchar *objects_uri;

      /* Old implementations might not override this property */
      g_object_get (cert, "pkcs11-uri", &objects_uri, NULL);
      if (objects_uri == NULL)
        {
          g_set_error_literal (error, G_IO_ERROR, G_IO_ERROR_NOT_SUPPORTED, _("This GTlsBackend does not support creating PKCS #11 certificates"));
          g_object_unref (cert);
          return NULL;
        }
      g_free (objects_uri);
    }

  return G_TLS_CERTIFICATE (cert);
}

/**
 * g_tls_certificate_list_new_from_file:
 * @file: (type filename): file containing PEM-encoded certificates to import
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Creates one or more #GTlsCertificates from the PEM-encoded
 * data in @file. If @file cannot be read or parsed, the function will
 * return %NULL and set @error. If @file does not contain any
 * PEM-encoded certificates, this will return an empty list and not
 * set @error.
 *
 * Returns: (element-type Gio.TlsCertificate) (transfer full): a
 * #GList containing #GTlsCertificate objects. You must free the list
 * and its contents when you are done with it.
 *
 * Since: 2.28
 */
GList *
g_tls_certificate_list_new_from_file (const gchar  *file,
				      GError      **error)
{
  GQueue queue = G_QUEUE_INIT;
  gchar *contents, *end;
  const gchar *p;
  gsize length;

  if (!g_file_get_contents (file, &contents, &length, error))
    return NULL;

  end = contents + length;
  p = contents;
  while (p && *p)
    {
      gchar *cert_pem;
      GTlsCertificate *cert = NULL;
      GError *parse_error = NULL;

      cert_pem = parse_next_pem_certificate (&p, end, FALSE, &parse_error);
      if (cert_pem)
        {
          cert = g_tls_certificate_new_internal (cert_pem, NULL, NULL, &parse_error);
          g_free (cert_pem);
        }
      if (!cert)
        {
          if (parse_error)
            {
              g_propagate_error (error, parse_error);
              g_list_free_full (queue.head, g_object_unref);
              queue.head = NULL;
            }
          break;
        }
      g_queue_push_tail (&queue, cert);
    }

  g_free (contents);
  return queue.head;
}


/**
 * g_tls_certificate_get_issuer:
 * @cert: a #GTlsCertificate
 *
 * Gets the #GTlsCertificate representing @cert's issuer, if known
 *
 * Returns: (nullable) (transfer none): The certificate of @cert's issuer,
 * or %NULL if @cert is self-signed or signed with an unknown
 * certificate.
 *
 * Since: 2.28
 */
GTlsCertificate *
g_tls_certificate_get_issuer (GTlsCertificate  *cert)
{
  GTlsCertificate *issuer;

  g_object_get (G_OBJECT (cert), "issuer", &issuer, NULL);
  if (issuer)
    g_object_unref (issuer);

  return issuer;
}

/**
 * g_tls_certificate_verify:
 * @cert: a #GTlsCertificate
 * @identity: (nullable): the expected peer identity
 * @trusted_ca: (nullable): the certificate of a trusted authority
 *
 * This verifies @cert and returns a set of #GTlsCertificateFlags
 * indicating any problems found with it. This can be used to verify a
 * certificate outside the context of making a connection, or to
 * check a certificate against a CA that is not part of the system
 * CA database.
 *
 * If @identity is not %NULL, @cert's name(s) will be compared against
 * it, and %G_TLS_CERTIFICATE_BAD_IDENTITY will be set in the return
 * value if it does not match. If @identity is %NULL, that bit will
 * never be set in the return value.
 *
 * If @trusted_ca is not %NULL, then @cert (or one of the certificates
 * in its chain) must be signed by it, or else
 * %G_TLS_CERTIFICATE_UNKNOWN_CA will be set in the return value. If
 * @trusted_ca is %NULL, that bit will never be set in the return
 * value.
 *
 * GLib guarantees that if certificate verification fails, at least one
 * error will be set in the return value, but it does not guarantee
 * that all possible errors will be set. Accordingly, you may not safely
 * decide to ignore any particular type of error. For example, it would
 * be incorrect to mask %G_TLS_CERTIFICATE_EXPIRED if you want to allow
 * expired certificates, because this could potentially be the only
 * error flag set even if other problems exist with the certificate.
 *
 * Because TLS session context is not used, #GTlsCertificate may not
 * perform as many checks on the certificates as #GTlsConnection would.
 * For example, certificate constraints may not be honored, and
 * revocation checks may not be performed. The best way to verify TLS
 * certificates used by a TLS connection is to let #GTlsConnection
 * handle the verification.
 *
 * Returns: the appropriate #GTlsCertificateFlags
 *
 * Since: 2.28
 */
GTlsCertificateFlags
g_tls_certificate_verify (GTlsCertificate     *cert,
			  GSocketConnectable  *identity,
			  GTlsCertificate     *trusted_ca)
{
  return G_TLS_CERTIFICATE_GET_CLASS (cert)->verify (cert, identity, trusted_ca);
}

/**
 * g_tls_certificate_is_same:
 * @cert_one: first certificate to compare
 * @cert_two: second certificate to compare
 *
 * Check if two #GTlsCertificate objects represent the same certificate.
 * The raw DER byte data of the two certificates are checked for equality.
 * This has the effect that two certificates may compare equal even if
 * their #GTlsCertificate:issuer, #GTlsCertificate:private-key, or
 * #GTlsCertificate:private-key-pem properties differ.
 *
 * Returns: whether the same or not
 *
 * Since: 2.34
 */
gboolean
g_tls_certificate_is_same (GTlsCertificate     *cert_one,
                           GTlsCertificate     *cert_two)
{
  GByteArray *b1, *b2;
  gboolean equal;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert_one), FALSE);
  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert_two), FALSE);

  g_object_get (cert_one, "certificate", &b1, NULL);
  g_object_get (cert_two, "certificate", &b2, NULL);

  equal = (b1->len == b2->len &&
           memcmp (b1->data, b2->data, b1->len) == 0);

  g_byte_array_unref (b1);
  g_byte_array_unref (b2);

  return equal;
}


/**
 * g_tls_certificate_get_not_valid_before:
 * @cert: a #GTlsCertificate
 *
 * Returns the time at which the certificate became or will become valid.
 *
 * Returns: (nullable) (transfer full): The not-valid-before date, or %NULL if it's not available.
 *
 * Since: 2.70
 */
GDateTime *
g_tls_certificate_get_not_valid_before (GTlsCertificate *cert)
{
  GDateTime *not_valid_before = NULL;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert), NULL);

  g_object_get (G_OBJECT (cert), "not-valid-before", &not_valid_before, NULL);

  return g_steal_pointer (&not_valid_before);
}

/**
 * g_tls_certificate_get_not_valid_after:
 * @cert: a #GTlsCertificate
 *
 * Returns the time at which the certificate became or will become invalid.
 *
 * Returns: (nullable) (transfer full): The not-valid-after date, or %NULL if it's not available.
 *
 * Since: 2.70
 */
GDateTime *
g_tls_certificate_get_not_valid_after (GTlsCertificate *cert)
{
  GDateTime *not_valid_after = NULL;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert), NULL);

  g_object_get (G_OBJECT (cert), "not-valid-after", &not_valid_after, NULL);

  return g_steal_pointer (&not_valid_after);
}

/**
 * g_tls_certificate_get_subject_name:
 * @cert: a #GTlsCertificate
 *
 * Returns the subject name from the certificate.
 *
 * Returns: (nullable) (transfer full): The subject name, or %NULL if it's not available.
 *
 * Since: 2.70
 */
gchar *
g_tls_certificate_get_subject_name (GTlsCertificate *cert)
{
  gchar *subject_name = NULL;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert), NULL);

  g_object_get (G_OBJECT (cert), "subject-name", &subject_name, NULL);

  return g_steal_pointer (&subject_name);
}

/**
 * g_tls_certificate_get_issuer_name:
 * @cert: a #GTlsCertificate
 *
 * Returns the issuer name from the certificate.
 *
 * Returns: (nullable) (transfer full): The issuer name, or %NULL if it's not available.
 *
 * Since: 2.70
 */
gchar *
g_tls_certificate_get_issuer_name (GTlsCertificate *cert)
{
  gchar *issuer_name = NULL;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert), NULL);

  g_object_get (G_OBJECT (cert), "issuer-name", &issuer_name, NULL);

  return g_steal_pointer (&issuer_name);
}

/**
 * g_tls_certificate_get_dns_names:
 * @cert: a #GTlsCertificate
 *
 * Gets the value of #GTlsCertificate:dns-names.
 *
 * Returns: (nullable) (element-type GBytes) (transfer container): A #GPtrArray of
 * #GBytes elements, or %NULL if it's not available.
 *
 * Since: 2.70
 */
GPtrArray *
g_tls_certificate_get_dns_names (GTlsCertificate *cert)
{
  GPtrArray *dns_names = NULL;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert), NULL);

  g_object_get (G_OBJECT (cert), "dns-names", &dns_names, NULL);

  return g_steal_pointer (&dns_names);
}

/**
 * g_tls_certificate_get_ip_addresses:
 * @cert: a #GTlsCertificate
 *
 * Gets the value of #GTlsCertificate:ip-addresses.
 *
 * Returns: (nullable) (element-type GInetAddress) (transfer container): A #GPtrArray
 * of #GInetAddress elements, or %NULL if it's not available.
 *
 * Since: 2.70
 */
GPtrArray *
g_tls_certificate_get_ip_addresses (GTlsCertificate *cert)
{
  GPtrArray *ip_addresses = NULL;

  g_return_val_if_fail (G_IS_TLS_CERTIFICATE (cert), NULL);

  g_object_get (G_OBJECT (cert), "ip-addresses", &ip_addresses, NULL);

  return g_steal_pointer (&ip_addresses);
}
