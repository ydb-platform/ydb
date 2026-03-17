/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

// Return the rank of the key using list of keys (For BUFR keys)
// The argument 'keys' is an input as well as output from each call
int compute_bufr_key_rank(grib_handle* h, grib_string_list* keys, const char* key)
{
    grib_string_list* next = keys;
    grib_string_list* prev = keys;
    int theRank            = 0;
    size_t size            = 0;
    const grib_context* c  = h->context;
    DEBUG_ASSERT(h->product_kind == PRODUCT_BUFR);

    while (next && next->value && strcmp(next->value, key)) {
        prev = next;
        next = next->next;
    }
    if (!next) {
        DEBUG_ASSERT(prev);
        if (prev) {
            prev->next = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
            next       = prev->next;
        }
    }
    DEBUG_ASSERT(next);
    if (!next) return 0;

    if (!next->value) {
        next->value = strdup(key);
        next->count = 0;
    }

    next->count++;
    theRank = next->count;
    if (theRank == 1) {
        // If the count is 1 it could mean two things:
        //   This is the first instance of the key and there is another one
        //   This is the first and only instance of the key
        // So we check if there is a second one of this key,
        // If not, then rank is zero i.e. this is the only instance
        size_t slen = strlen(key) + 5;
        char* s = (char*)grib_context_malloc_clear(c, slen);
        snprintf(s, slen, "#2#%s", key);
        if (grib_get_size(h, s, &size) == GRIB_NOT_FOUND)
            theRank = 0;
        grib_context_free(c, s);
    }

    return theRank;
}

char** codes_bufr_copy_data_return_copied_keys(grib_handle* hin, grib_handle* hout, size_t* nkeys, int* err)
{
    bufr_keys_iterator* kiter = NULL;
    char** keys               = NULL;
    grib_sarray* k            = 0;

    if (hin == NULL || hout == NULL) {
        *err = GRIB_NULL_HANDLE;
        return NULL;
    }

    kiter = codes_bufr_data_section_keys_iterator_new(hin);
    if (!kiter)
        return NULL;
    k = grib_sarray_new(50, 10);

    while (codes_bufr_keys_iterator_next(kiter)) {
        char* name = codes_bufr_keys_iterator_get_name(kiter);
        // if the copy fails we want to keep copying without any errors.
        //   This is because the copy can be between structures that are not
        //   identical and we want to copy what can be copied and skip what
        //   cannot be copied because is not in the output handle
        *err = codes_copy_key(hin, hout, name, 0);
        if (*err == 0) {
            // 'name' will be freed when we call codes_bufr_keys_iterator_delete so copy
            char* copied_name = strdup(name);
            k = grib_sarray_push(k, copied_name);
        }
    }
    *nkeys = grib_sarray_used_size(k);
    keys   = grib_sarray_get_array(k);
    grib_sarray_delete(k);
    if (*nkeys > 0) {
        // Do the pack if something was copied
        *err = grib_set_long(hout, "pack", 1);
    }
    codes_bufr_keys_iterator_delete(kiter);
    return keys;
}

int codes_bufr_copy_data(grib_handle* hin, grib_handle* hout)
{
    bufr_keys_iterator* kiter = NULL;
    int err                   = 0;
    int nkeys                 = 0;

    if (hin == NULL || hout == NULL) {
        return GRIB_NULL_HANDLE;
    }

    kiter = codes_bufr_data_section_keys_iterator_new(hin);
    if (!kiter)
        return GRIB_INTERNAL_ERROR;

    while (codes_bufr_keys_iterator_next(kiter)) {
        char* name = codes_bufr_keys_iterator_get_name(kiter);
        // if the copy fails we want to keep copying without any error messages.
        //   This is because the copy can be between structures that are not
        //   identical and we want to copy what can be copied and skip what
        //   cannot be copied because is not in the output handle
        err = codes_copy_key(hin, hout, name, GRIB_TYPE_UNDEFINED);
        if (err == 0)
            nkeys++;
    }

    if (nkeys > 0) {
        // Do the pack if something was copied
        err = grib_set_long(hout, "pack", 1);
    }

    codes_bufr_keys_iterator_delete(kiter);
    return err;
}

#define BUFR_SECTION0_LEN 8 // BUFR section 0 is always 8 bytes long
static int bufr_extract_edition(const void* message, long* edition)
{
    const long nbits_edition = 8;
    long pos_edition         = 7 * 8;
    const unsigned char* pMessage = (const unsigned char*)message;

    *edition = (long)grib_decode_unsigned_long(pMessage, &pos_edition, nbits_edition);
    return GRIB_SUCCESS;
}
// The ECMWF BUFR local use section
static int bufr_decode_rdb_keys(const void* message, long offset_section2, codes_bufr_header* hdr)
{
    const unsigned char* pMessage = (const unsigned char*)message;
    long nbits_rdbType    = 1 * 8;
    long pos_rdbType      = (offset_section2 + 4) * 8;
    long nbits_oldSubtype = 1 * 8;
    long pos_oldSubtype   = (offset_section2 + 5) * 8;

    long nbits_qualityControl = 1 * 8;
    long pos_qualityControl   = (offset_section2 + 48) * 8;
    long nbits_newSubtype     = 2 * 8;
    long pos_newSubtype       = (offset_section2 + 49) * 8;
    long nbits_daLoop         = 1 * 8;
    long pos_daLoop           = (offset_section2 + 51) * 8;

    long start                = 0;
    const long offset_keyData = offset_section2 + 6;
    const long offset_rdbtime = offset_section2 + 38;
    const long offset_rectime = offset_section2 + 41;

    unsigned char* p = (unsigned char*)message + offset_keyData;

    DEBUG_ASSERT(hdr->ecmwfLocalSectionPresent);

    hdr->rdbType    = (long)grib_decode_unsigned_long(pMessage, &pos_rdbType, nbits_rdbType);
    hdr->oldSubtype = (long)grib_decode_unsigned_long(pMessage, &pos_oldSubtype, nbits_oldSubtype);

    start            = 0;
    hdr->localYear   = (long)grib_decode_unsigned_long(p, &start, 12);
    hdr->localMonth  = (long)grib_decode_unsigned_long(p, &start, 4);
    hdr->localDay    = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->localHour   = (long)grib_decode_unsigned_long(p, &start, 5);
    hdr->localMinute = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->localSecond = (long)grib_decode_unsigned_long(p, &start, 6);

    // rdbtime
    p                  = (unsigned char*)message + offset_rdbtime;
    start              = 0;
    hdr->rdbtimeDay    = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->rdbtimeHour   = (long)grib_decode_unsigned_long(p, &start, 5);
    hdr->rdbtimeMinute = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->rdbtimeSecond = (long)grib_decode_unsigned_long(p, &start, 6);

    // rectime
    p                  = (unsigned char*)message + offset_rectime;
    start              = 0;
    hdr->rectimeDay    = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->rectimeHour   = (long)grib_decode_unsigned_long(p, &start, 5);
    hdr->rectimeMinute = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->rectimeSecond = (long)grib_decode_unsigned_long(p, &start, 6);
    hdr->restricted    = (long)grib_decode_unsigned_long(p, &start, 1);

    hdr->qualityControl = (long)grib_decode_unsigned_long(pMessage, &pos_qualityControl, nbits_qualityControl);
    hdr->newSubtype     = (long)grib_decode_unsigned_long(pMessage, &pos_newSubtype, nbits_newSubtype);
    hdr->daLoop         = (long)grib_decode_unsigned_long(pMessage, &pos_daLoop, nbits_daLoop);
    hdr->rdbSubtype     = (hdr->oldSubtype < 255) ? hdr->oldSubtype : hdr->newSubtype;

    return GRIB_SUCCESS;
}

#define IDENT_LEN 9 // 8 chars plus the final 0 terminator

// The ECMWF BUFR local use section
static int bufr_decode_extra_rdb_keys(const void* message, long offset_section2, codes_bufr_header* hdr)
{
    bool isSatelliteType      = false;
    long start                = 0;
    const long offset_keyData = offset_section2 + 6;
    const long offset_keyMore = offset_section2 + 19; // 8 bytes long
    const long offset_keySat  = offset_section2 + 27; // 9 bytes long

    unsigned char* pKeyData = (unsigned char*)message + offset_keyData;
    char* pKeyMore          = (char*)message + offset_keyMore;

    DEBUG_ASSERT(hdr->ecmwfLocalSectionPresent);

    if (hdr->rdbType == 2 || hdr->rdbType == 3 || hdr->rdbType == 8 || hdr->rdbType == 12 || hdr->rdbType == 30) {
        isSatelliteType = true;
    }
    if (isSatelliteType || hdr->numberOfSubsets > 1) {
        hdr->isSatellite = 1;
    }
    else {
        hdr->isSatellite = 0;
    }

    if (hdr->isSatellite) {
        unsigned char* pKeyMoreLong = (unsigned char*)message + offset_keyMore; // as an integer
        unsigned char* pKeySat      = (unsigned char*)message + offset_keySat;
        unsigned long lValue        = 0;
        start                       = 40;
        lValue                      = grib_decode_unsigned_long(pKeyData, &start, 26);
        hdr->localLongitude1        = (lValue - 18000000.0) / 100000.0;
        start                       = 72;
        lValue                      = grib_decode_unsigned_long(pKeyData, &start, 25);
        hdr->localLatitude1         = (lValue - 9000000.0) / 100000.0;
        start                       = 0;
        lValue                      = grib_decode_unsigned_long(pKeyMoreLong, &start, 26);
        hdr->localLongitude2        = (lValue - 18000000.0) / 100000.0;
        start                       = 32;
        lValue                      = grib_decode_unsigned_long(pKeyMoreLong, &start, 25);
        hdr->localLatitude2         = (lValue - 9000000.0) / 100000.0;

        if (hdr->oldSubtype == 255 || hdr->numberOfSubsets > 255 ||
            (hdr->oldSubtype >= 121 && hdr->oldSubtype <= 130) ||
            hdr->oldSubtype == 31) {
            start                          = 0;
            hdr->localNumberOfObservations = (long)grib_decode_unsigned_long(pKeySat, &start, 16);
            start                          = 16;
            hdr->satelliteID               = (long)grib_decode_unsigned_long(pKeySat, &start, 16);
        }
        else {
            start                          = 0;
            hdr->localNumberOfObservations = (long)grib_decode_unsigned_long(pKeySat, &start, 8);
            start                          = 8;
            hdr->satelliteID               = (long)grib_decode_unsigned_long(pKeySat, &start, 16);
        }
    }
    else {
        size_t i = 0;
        unsigned long lValue = 0;
        char* pTemp = NULL;
        char temp[IDENT_LEN] = {0,};

        start               = 72;
        lValue              = grib_decode_unsigned_long(pKeyData, &start, 25);
        hdr->localLatitude  = (lValue - 9000000.0) / 100000.0;
        start               = 40;
        lValue              = grib_decode_unsigned_long(pKeyData, &start, 26);
        hdr->localLongitude = (lValue - 18000000.0) / 100000.0;

        // interpret keyMore as a string. Copy to a temporary
        for (i = 0; i < IDENT_LEN - 1; ++i) {
            temp[i] = *pKeyMore++;
        }
        temp[i] = '\0';
        pTemp = temp;
        string_lrtrim(&pTemp, 1, 1); // Trim left and right
        strncpy(hdr->ident, pTemp, IDENT_LEN - 1);
    }

    return GRIB_SUCCESS;
}

static int bufr_decode_edition3(const void* message, codes_bufr_header* hdr)
{
    int err = GRIB_SUCCESS;
    const unsigned char* pMessage = (const unsigned char*)message;

    unsigned long totalLength    = 0;
    const long nbits_totalLength = 3 * 8;
    long pos_totalLength         = 4 * 8;

    unsigned long section1Length    = 0;
    const long nbits_section1Length = 3 * 8;
    long pos_section1Length         = 8 * 8;

    long nbits_masterTableNumber = 1 * 8;
    long pos_masterTableNumber   = 11 * 8;

    long nbits_bufrHeaderSubCentre = 1 * 8;
    long pos_bufrHeaderSubCentre   = 12 * 8;

    long nbits_bufrHeaderCentre = 1 * 8;
    long pos_bufrHeaderCentre   = 13 * 8;

    long nbits_updateSequenceNumber = 1 * 8;
    long pos_updateSequenceNumber   = 14 * 8;

    long section1Flags       = 0;
    long nbits_section1Flags = 1 * 8;
    long pos_section1Flags   = 15 * 8;

    long nbits_dataCategory = 1 * 8;
    long pos_dataCategory   = 16 * 8;

    long nbits_dataSubCategory = 1 * 8;
    long pos_dataSubCategory   = 17 * 8;

    long nbits_masterTablesVersionNumber = 1 * 8;
    long pos_masterTablesVersionNumber   = 18 * 8;

    long nbits_localTablesVersionNumber = 1 * 8;
    long pos_localTablesVersionNumber   = 19 * 8;

    const long typicalCentury       = 21; // This century
    long typicalYearOfCentury       = 0;
    long nbits_typicalYearOfCentury = 1 * 8;
    long pos_typicalYearOfCentury   = 20 * 8;

    long nbits_typicalMonth = 1 * 8;
    long pos_typicalMonth   = 21 * 8;

    long nbits_typicalDay = 1 * 8;
    long pos_typicalDay   = 22 * 8;

    long nbits_typicalHour = 1 * 8;
    long pos_typicalHour   = 23 * 8;

    long nbits_typicalMinute = 1 * 8;
    long pos_typicalMinute   = 24 * 8;

    long section2Length        = 0;
    long offset_section2       = 0;
    long offset_section3       = 0;
    long nbits_numberOfSubsets = 2 * 8;
    long pos_numberOfSubsets   = 0; //depends on offset_section3

    unsigned long section3Flags;
    long nbits_section3Flags = 1 * 8;
    long pos_section3Flags   = 0; //depends on offset_section3

    totalLength = grib_decode_unsigned_long(pMessage, &pos_totalLength, nbits_totalLength);
    if (totalLength != hdr->message_size) {
        return GRIB_WRONG_LENGTH;
    }
    section1Length                 = grib_decode_unsigned_long(pMessage, &pos_section1Length, nbits_section1Length);
    hdr->masterTableNumber         = (long)grib_decode_unsigned_long(pMessage, &pos_masterTableNumber, nbits_masterTableNumber);
    hdr->bufrHeaderSubCentre       = (long)grib_decode_unsigned_long(pMessage, &pos_bufrHeaderSubCentre, nbits_bufrHeaderSubCentre);
    hdr->bufrHeaderCentre          = (long)grib_decode_unsigned_long(pMessage, &pos_bufrHeaderCentre, nbits_bufrHeaderCentre);
    hdr->updateSequenceNumber      = (long)grib_decode_unsigned_long(pMessage, &pos_updateSequenceNumber, nbits_updateSequenceNumber);
    section1Flags                  = (long)grib_decode_unsigned_long(pMessage, &pos_section1Flags, nbits_section1Flags);
    hdr->dataCategory              = (long)grib_decode_unsigned_long(pMessage, &pos_dataCategory, nbits_dataCategory);
    hdr->dataSubCategory           = (long)grib_decode_unsigned_long(pMessage, &pos_dataSubCategory, nbits_dataSubCategory);
    hdr->masterTablesVersionNumber = (long)grib_decode_unsigned_long(
        pMessage, &pos_masterTablesVersionNumber, nbits_masterTablesVersionNumber);
    hdr->localTablesVersionNumber = (long)grib_decode_unsigned_long(pMessage, &pos_localTablesVersionNumber, nbits_localTablesVersionNumber);
    typicalYearOfCentury          = (long)grib_decode_unsigned_long(pMessage, &pos_typicalYearOfCentury, nbits_typicalYearOfCentury);
    hdr->typicalYear              = (typicalCentury - 1) * 100 + typicalYearOfCentury;
    hdr->typicalMonth             = (long)grib_decode_unsigned_long(pMessage, &pos_typicalMonth, nbits_typicalMonth);
    hdr->typicalDay               = (long)grib_decode_unsigned_long(pMessage, &pos_typicalDay, nbits_typicalDay);
    hdr->typicalHour              = (long)grib_decode_unsigned_long(pMessage, &pos_typicalHour, nbits_typicalHour);
    hdr->typicalMinute            = (long)grib_decode_unsigned_long(pMessage, &pos_typicalMinute, nbits_typicalMinute);
    hdr->typicalSecond            = 0;
    hdr->typicalDate              = hdr->typicalYear * 10000 + hdr->typicalMonth * 100 + hdr->typicalDay;
    hdr->typicalTime              = hdr->typicalHour * 10000 + hdr->typicalMinute * 100 + hdr->typicalSecond;

    offset_section2          = BUFR_SECTION0_LEN + section1Length; //bytes
    section2Length           = 0;
    hdr->localSectionPresent = (section1Flags != 0);
    if (hdr->localSectionPresent) {
        long pos_section2Length;
        const long nbits_section2Length = 3 * 8;
        pos_section2Length              = offset_section2 * 8;

        section2Length = grib_decode_unsigned_long(pMessage, &pos_section2Length, nbits_section2Length);

        if (hdr->bufrHeaderCentre == 98) {
            hdr->ecmwfLocalSectionPresent = 1;
            err                           = bufr_decode_rdb_keys(message, offset_section2, hdr);
        }
    }

    offset_section3       = BUFR_SECTION0_LEN + section1Length + section2Length; //bytes
    pos_numberOfSubsets   = (offset_section3 + 4) * 8;
    hdr->numberOfSubsets  = grib_decode_unsigned_long(pMessage, &pos_numberOfSubsets, nbits_numberOfSubsets);

    pos_section3Flags   = (offset_section3 + 6) * 8;
    section3Flags       = grib_decode_unsigned_long(pMessage, &pos_section3Flags, nbits_section3Flags);
    hdr->observedData   = (section3Flags & 1 << 7) ? 1 : 0;
    hdr->compressedData = (section3Flags & 1 << 6) ? 1 : 0;

    if (hdr->ecmwfLocalSectionPresent && hdr->bufrHeaderCentre == 98 && section2Length == 52) {
        err = bufr_decode_extra_rdb_keys(message, offset_section2, hdr);
    }

    return err;
}

static int bufr_decode_edition4(const void* message, codes_bufr_header* hdr)
{
    int err = GRIB_SUCCESS;
    const unsigned char* pMessage = (const unsigned char*)message;

    unsigned long totalLength    = 0;
    const long nbits_totalLength = 3 * 8;
    long pos_totalLength         = 4 * 8;

    unsigned long section1Length;
    const long nbits_section1Length = 3 * 8;
    long pos_section1Length         = 8 * 8;

    long nbits_masterTableNumber = 1 * 8;
    long pos_masterTableNumber   = 11 * 8;

    long nbits_bufrHeaderCentre = 2 * 8;
    long pos_bufrHeaderCentre   = 12 * 8;

    long nbits_bufrHeaderSubCentre = 2 * 8;
    long pos_bufrHeaderSubCentre   = 14 * 8;

    long nbits_updateSequenceNumber = 1 * 8;
    long pos_updateSequenceNumber   = 16 * 8;

    long section1Flags       = 0;
    long nbits_section1Flags = 1 * 8;
    long pos_section1Flags   = 17 * 8;

    long nbits_dataCategory = 1 * 8;
    long pos_dataCategory   = 18 * 8;

    long nbits_internationalDataSubCategory = 1 * 8;
    long pos_internationalDataSubCategory   = 19 * 8;

    long nbits_dataSubCategory = 1 * 8;
    long pos_dataSubCategory   = 20 * 8;

    long nbits_masterTablesVersionNumber = 1 * 8;
    long pos_masterTablesVersionNumber   = 21 * 8;

    long nbits_localTablesVersionNumber = 1 * 8;
    long pos_localTablesVersionNumber   = 22 * 8;

    long typicalYear2      = 0; // corrected
    long nbits_typicalYear = 2 * 8;
    long pos_typicalYear   = 23 * 8;

    long nbits_typicalMonth = 1 * 8;
    long pos_typicalMonth   = 25 * 8;

    long nbits_typicalDay = 1 * 8;
    long pos_typicalDay   = 26 * 8;

    long nbits_typicalHour = 1 * 8;
    long pos_typicalHour   = 27 * 8;

    long nbits_typicalMinute = 1 * 8;
    long pos_typicalMinute   = 28 * 8;

    long nbits_typicalSecond = 1 * 8;
    long pos_typicalSecond   = 29 * 8;

    unsigned long section2Length = 0;
    long offset_section2       = 0;
    long offset_section3       = 0;
    long nbits_numberOfSubsets = 2 * 8;
    long pos_numberOfSubsets   = 0; //depends on offset_section3

    unsigned long section3Flags;
    long nbits_section3Flags = 1 * 8;
    long pos_section3Flags   = 0; //depends on offset_section3

    totalLength = grib_decode_unsigned_long(pMessage, &pos_totalLength, nbits_totalLength);
    if (totalLength != hdr->message_size) {
        return GRIB_WRONG_LENGTH;
    }
    section1Length                    = grib_decode_unsigned_long(pMessage, &pos_section1Length, nbits_section1Length);
    hdr->masterTableNumber            = (long)grib_decode_unsigned_long(pMessage, &pos_masterTableNumber, nbits_masterTableNumber);
    hdr->bufrHeaderCentre             = (long)grib_decode_unsigned_long(pMessage, &pos_bufrHeaderCentre, nbits_bufrHeaderCentre);
    hdr->bufrHeaderSubCentre          = (long)grib_decode_unsigned_long(pMessage, &pos_bufrHeaderSubCentre, nbits_bufrHeaderSubCentre);
    hdr->updateSequenceNumber         = (long)grib_decode_unsigned_long(pMessage, &pos_updateSequenceNumber, nbits_updateSequenceNumber);
    section1Flags                     = (long)grib_decode_unsigned_long(pMessage, &pos_section1Flags, nbits_section1Flags);
    hdr->dataCategory                 = (long)grib_decode_unsigned_long(pMessage, &pos_dataCategory, nbits_dataCategory);
    hdr->internationalDataSubCategory = (long)grib_decode_unsigned_long(pMessage, &pos_internationalDataSubCategory, nbits_internationalDataSubCategory);
    hdr->dataSubCategory              = (long)grib_decode_unsigned_long(pMessage, &pos_dataSubCategory, nbits_dataSubCategory);
    hdr->masterTablesVersionNumber    = (long)grib_decode_unsigned_long(pMessage, &pos_masterTablesVersionNumber, nbits_masterTablesVersionNumber);
    hdr->localTablesVersionNumber     = (long)grib_decode_unsigned_long(pMessage, &pos_localTablesVersionNumber, nbits_localTablesVersionNumber);

    hdr->typicalYear   = (long)grib_decode_unsigned_long(pMessage, &pos_typicalYear, nbits_typicalYear);
    typicalYear2       = hdr->typicalYear < 100 ? 2000 + hdr->typicalYear : hdr->typicalYear; //ECC-556
    hdr->typicalMonth  = (long)grib_decode_unsigned_long(pMessage, &pos_typicalMonth, nbits_typicalMonth);
    hdr->typicalDay    = (long)grib_decode_unsigned_long(pMessage, &pos_typicalDay, nbits_typicalDay);
    hdr->typicalHour   = (long)grib_decode_unsigned_long(pMessage, &pos_typicalHour, nbits_typicalHour);
    hdr->typicalMinute = (long)grib_decode_unsigned_long(pMessage, &pos_typicalMinute, nbits_typicalMinute);
    hdr->typicalSecond = (long)grib_decode_unsigned_long(pMessage, &pos_typicalSecond, nbits_typicalSecond);
    hdr->typicalDate   = typicalYear2 * 10000 + hdr->typicalMonth * 100 + hdr->typicalDay;
    hdr->typicalTime   = hdr->typicalHour * 10000 + hdr->typicalMinute * 100 + hdr->typicalSecond;

    offset_section2          = BUFR_SECTION0_LEN + section1Length; //bytes
    section2Length           = 0;
    hdr->localSectionPresent = (section1Flags != 0);
    if (hdr->localSectionPresent) {
        long pos_section2Length;
        const long nbits_section2Length = 3 * 8;
        pos_section2Length              = offset_section2 * 8;

        section2Length = grib_decode_unsigned_long(pMessage, &pos_section2Length, nbits_section2Length);

        if (hdr->bufrHeaderCentre == 98) {
            hdr->ecmwfLocalSectionPresent = 1;
            err                           = bufr_decode_rdb_keys(message, offset_section2, hdr);
        }
    }

    offset_section3       = BUFR_SECTION0_LEN + section1Length + section2Length; //bytes
    pos_numberOfSubsets   = (offset_section3 + 4) * 8;
    hdr->numberOfSubsets  = grib_decode_unsigned_long(pMessage, &pos_numberOfSubsets, nbits_numberOfSubsets);

    pos_section3Flags   = (offset_section3 + 6) * 8;
    section3Flags       = grib_decode_unsigned_long(pMessage, &pos_section3Flags, nbits_section3Flags);
    hdr->observedData   = (section3Flags & 1 << 7) ? 1 : 0;
    hdr->compressedData = (section3Flags & 1 << 6) ? 1 : 0;

    if (hdr->ecmwfLocalSectionPresent && hdr->bufrHeaderCentre == 98 && section2Length == 52) {
        err = bufr_decode_extra_rdb_keys(message, offset_section2, hdr);
    }

    return err;
}

static int bufr_decode_header(grib_context* c, const void* message, off_t offset, size_t size, codes_bufr_header* hdr)
{
    int err = GRIB_SUCCESS;

    hdr->message_offset = (unsigned long)offset;
    hdr->message_size   = (unsigned long)size;

    err = bufr_extract_edition(message, &hdr->edition);
    if (err) return err;

    if (hdr->edition == 3) {
        err = bufr_decode_edition3(message, hdr);
    }
    else if (hdr->edition == 4) {
        err = bufr_decode_edition4(message, hdr);
    }
    else {
        grib_context_log(c, GRIB_LOG_ERROR, "Unsupported BUFR edition: %ld", hdr->edition);
        err = GRIB_DECODING_ERROR;
    }

    return err;
}

static int count_bufr_messages(grib_context* c, FILE* f, int* n, int strict_mode)
{
    int err      = 0;
    void* mesg   = NULL;
    size_t size  = 0;
    off_t offset = 0;
    int done     = 0;

    *n = 0;
    if (!c)
        c = grib_context_get_default();

    while (!done) {
        mesg = wmo_read_bufr_from_file_malloc(f, 0, &size, &offset, &err);
        //printf("Count so far=%d, mesg=%x, err=%d (%s)\n", *n, mesg, err, grib_get_error_message(err));
        if (!mesg) {
            if (err == GRIB_END_OF_FILE || err == GRIB_PREMATURE_END_OF_FILE) {
                done = 1; // reached the end
                break;
            }
            if (strict_mode)
                return GRIB_DECODING_ERROR;
        }
        if (mesg && !err) {
            grib_context_free(c, mesg);
        }
        (*n)++;
        if (*n >= INT_MAX/100) {
            grib_context_log(c, GRIB_LOG_ERROR, "Limit reached: looped %d times without finding a valid BUFR message", *n);
            done = 1;
            err = GRIB_INTERNAL_ERROR;
        }
    }
    (void)done;
    rewind(f);
    if (err == GRIB_END_OF_FILE)
        err = GRIB_SUCCESS;
    return err;
}

int codes_bufr_extract_headers_malloc(grib_context* c, const char* filename, codes_bufr_header** result, int* num_messages, int strict_mode)
{
    int err = 0, i = 0;
    FILE* fp     = NULL;
    void* mesg   = NULL;
    size_t size  = 0;
    off_t offset = 0;

    if (!c)
        c = grib_context_get_default();
    if (path_is_directory(filename)) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: \"%s\" is a directory", __func__, filename);
        return GRIB_IO_PROBLEM;
    }
    fp = fopen(filename, "rb");
    if (!fp) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to read file \"%s\"", __func__, filename);
        perror(filename);
        return GRIB_IO_PROBLEM;
    }
    err = count_bufr_messages(c, fp, num_messages, strict_mode);
    if (err) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to count BUFR messages in file \"%s\"", __func__, filename);
        fclose(fp);
        return err;
    }

    size = *num_messages;
    if (size == 0) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: No BUFR messages in file \"%s\"", __func__, filename);
        return GRIB_INVALID_MESSAGE;
    }
    *result = (codes_bufr_header*)calloc(size, sizeof(codes_bufr_header));
    if (!*result) {
        fclose(fp);
        return GRIB_OUT_OF_MEMORY;
    }
    i = 0;
    while (err != GRIB_END_OF_FILE) {
        if (i >= *num_messages)
            break;
        mesg = wmo_read_bufr_from_file_malloc(fp, 0, &size, &offset, &err);
        if (mesg != NULL && err == 0) {
            int err2 = bufr_decode_header(c, mesg, offset, size, &(*result)[i]);
            if (err2) {
                fclose(fp);
                return err2;
            }
            grib_context_free(c, mesg);
        }
        if (mesg && err) {
            if (strict_mode) {
                fclose(fp);
                grib_context_free(c, mesg);
                return GRIB_DECODING_ERROR;
            }
        }
        if (!mesg) {
            if (err != GRIB_END_OF_FILE && err != GRIB_PREMATURE_END_OF_FILE) {
                // An error occurred
                grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to read BUFR message", __func__);
                if (strict_mode) {
                    fclose(fp);
                    return GRIB_DECODING_ERROR;
                }
            }
        }
        ++i;
    }

    fclose(fp);
    return GRIB_SUCCESS;
}

static const char* codes_bufr_header_get_centre_name(long centre_code)
{
    switch (centre_code) {
        case 1:
            return "ammc";
        case 4:
            return "rums";
        case 7:
            return "kwbc";
        case 24:
            return "fapr";
        case 28:
            return "vabb";
        case 29:
            return "dems";
        case 34:
            return "rjtd";
        case 38:
            return "babj";
        case 40:
            return "rksl";
        case 41:
            return "sabm";
        case 46:
            return "sbsj";
        case 54:
            return "cwao";
        case 58:
            return "fnmo";
        case 69:
            return "nzkl";
        case 74:
            return "egrr";
        case 78:
            return "edzw";
        case 80:
            return "cnmc";
        case 82:
            return "eswi";
        case 84:
            return "lfpw";
        case 85:
            return "lfpw";
        case 86:
            return "efkl";
        case 88:
            return "enmi";
        case 94:
            return "ekmi";
        case 98:
            return "ecmf";
        case 173:
            return "nasa";
        case 195:
            return "wiix";
        case 204:
            return "niwa";
        case 213:
            return "birk";
        case 214:
            return "lemm";
        case 215:
            return "lssw";
        case 218:
            return "habp";
        case 224:
            return "lowm";
        case 227:
            return "ebum";
        case 233:
            return "eidb";
        case 235:
            return "ingv";
        case 239:
            return "crfc";
        case 99:
            return "knmi";
        case 250:
            return "cosmo";
        case 252:
            return "mpim";
        case 254:
            return "eums";
        case 255:
            return "consensus";
        case 291:
            return "anso";
        case 292:
            return "ufz";
        default:
            return NULL;
    }
}

#if defined(BUFR_PROCESS_CODE_TABLE)
// TODO(masn): Not efficient as it opens the code table every time
static char* codes_bufr_header_get_centre_name(long edition, long centre_code)
{
    char full_path[2014] = {0,};
    char line[1024];
    FILE *f = NULL;
    const char* defs_path = grib_definition_path(NULL);

    if      (edition == 3) snprintf(full_path, 2014, "%s/common/c-1.table", defs_path);
    else if (edition == 4) snprintf(full_path, 2014, "%s/common/c-11.table", defs_path);
    else return NULL;

    f = codes_fopen(full_path, "r");
    if (!f) return NULL;

    while(fgets(line,sizeof(line)-1,f)) {
        char* p = line;
        int code = 0;
        char abbreviation[32] = {0,};
        char* q = abbreviation;

        line[strlen(line)-1] = 0;

        while(*p != '\0' && isspace(*p)) p++;

        if(*p == '#')
            continue;

        while(*p != '\0' && isspace(*p)) p++;

        if( *p =='\0' ) continue;

        ECCODES_ASSERT(isdigit(*p));
        while(*p != '\0') {
            if(isspace(*p)) break;
            code *= 10;
            code += *p - '0';
            p++;
        }

        while(*p != '\0' && isspace(*p)) p++;

        while(*p != '\0') {
            if(isspace(*p)) break;
            *q++ = *p++;
        }
        *q = 0;
        if (code == centre_code) {
            fclose(f);
            return strdup(abbreviation);
        }
    }
    fclose(f);
    return NULL;
}
#endif

int codes_bufr_header_get_string(codes_bufr_header* bh, const char* key, char* val, size_t* len)
{
    static const char* NOT_FOUND = "not_found";
    bool isEcmwfLocal            = false;
    ECCODES_ASSERT(bh);
    ECCODES_ASSERT(key);
    *len = strlen(NOT_FOUND); // By default

    isEcmwfLocal = (bh->ecmwfLocalSectionPresent == 1);
    ECCODES_ASSERT(!(isEcmwfLocal && bh->bufrHeaderCentre != 98));
    ECCODES_ASSERT(!(bh->ecmwfLocalSectionPresent && !bh->localSectionPresent));

    if (strcmp(key, "message_offset") == 0)
        *len = snprintf(val, 32, "%lu", bh->message_offset);
    else if (strcmp(key, "offset") == 0)
        *len = snprintf(val, 32, "%lu", bh->message_offset);
    else if (strcmp(key, "message_size") == 0)
        *len = snprintf(val, 32, "%lu", bh->message_size);
    else if (strcmp(key, "totalLength") == 0)
        *len = snprintf(val, 32, "%lu", bh->message_size);
    else if (strcmp(key, "edition") == 0)
        *len = snprintf(val, 32, "%ld", bh->edition);
    else if (strcmp(key, "masterTableNumber") == 0)
        *len = snprintf(val, 32, "%ld", bh->masterTableNumber);
    else if (strcmp(key, "bufrHeaderSubCentre") == 0)
        *len = snprintf(val, 32, "%ld", bh->bufrHeaderSubCentre);
    else if (strcmp(key, "bufrHeaderCentre") == 0)
        *len = snprintf(val, 32, "%ld", bh->bufrHeaderCentre);

    else if (strcmp(key, "centre") == 0) {
        const char* centre_str = codes_bufr_header_get_centre_name(bh->bufrHeaderCentre);
        if (centre_str)
            *len = snprintf(val, 32, "%s", centre_str);
        else
            *len = snprintf(val, 32, "%ld", bh->bufrHeaderCentre);
    }

    else if (strcmp(key, "updateSequenceNumber") == 0)
        *len = snprintf(val, 32, "%ld", bh->updateSequenceNumber);
    else if (strcmp(key, "dataCategory") == 0)
        *len = snprintf(val, 32, "%ld", bh->dataCategory);
    else if (strcmp(key, "dataSubCategory") == 0)
        *len = snprintf(val, 32, "%ld", bh->dataSubCategory);
    else if (strcmp(key, "masterTablesVersionNumber") == 0)
        *len = snprintf(val, 32, "%ld", bh->masterTablesVersionNumber);
    else if (strcmp(key, "localTablesVersionNumber") == 0)
        *len = snprintf(val, 32, "%ld", bh->localTablesVersionNumber);
    else if (strcmp(key, "typicalYear") == 0)
        *len = snprintf(val, 32, "%ld", bh->typicalYear);
    else if (strcmp(key, "typicalMonth") == 0)
        *len = snprintf(val, 32, "%ld", bh->typicalMonth);
    else if (strcmp(key, "typicalDay") == 0)
        *len = snprintf(val, 32, "%ld", bh->typicalDay);
    else if (strcmp(key, "typicalHour") == 0)
        *len = snprintf(val, 32, "%ld", bh->typicalHour);
    else if (strcmp(key, "typicalMinute") == 0)
        *len = snprintf(val, 32, "%ld", bh->typicalMinute);
    else if (strcmp(key, "typicalSecond") == 0)
        *len = snprintf(val, 32, "%ld", bh->typicalSecond);
    else if (strcmp(key, "typicalDate") == 0)
        *len = snprintf(val, 32, "%06ld", bh->typicalDate);
    else if (strcmp(key, "typicalTime") == 0)
        *len = snprintf(val, 32, "%06ld", bh->typicalTime);
    else if (strcmp(key, "internationalDataSubCategory") == 0)
        *len = snprintf(val, 32, "%ld", bh->internationalDataSubCategory);
    else if (strcmp(key, "localSectionPresent") == 0)
        *len = snprintf(val, 32, "%ld", bh->localSectionPresent);
    else if (strcmp(key, "ecmwfLocalSectionPresent") == 0)
        *len = snprintf(val, 32, "%ld", bh->ecmwfLocalSectionPresent);

    // Local ECMWF keys. Can be absent so must return NOT_FOUND
    else if (strcmp(key, "rdbType") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rdbType);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "oldSubtype") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->oldSubtype);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "ident") == 0) {
        if (!isEcmwfLocal || strlen(bh->ident) == 0)
            strcpy(val, NOT_FOUND);
        else
            *len = snprintf(val, 32, "%s", bh->ident);
    }
    else if (strcmp(key, "localYear") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localYear);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localMonth") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localMonth);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localDay") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localDay);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localHour") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localHour);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localMinute") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localMinute);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localSecond") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localSecond);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rdbtimeDay") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rdbtimeDay);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rdbtimeHour") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rdbtimeHour);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rdbtimeMinute") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rdbtimeMinute);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rdbtimeSecond") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rdbtimeSecond);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rectimeDay") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rectimeDay);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rectimeHour") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rectimeHour);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rectimeMinute") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rectimeMinute);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rectimeSecond") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rectimeSecond);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "restricted") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->restricted);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "isSatellite") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->isSatellite);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localLongitude1") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%g", bh->localLongitude1);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localLatitude1") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%g", bh->localLatitude1);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localLongitude2") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%g", bh->localLongitude2);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localLatitude2") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%g", bh->localLatitude2);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localLatitude") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%g", bh->localLatitude);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localLongitude") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%g", bh->localLongitude);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "qualityControl") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->qualityControl);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "newSubtype") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->newSubtype);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "rdbSubtype") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->rdbSubtype);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "daLoop") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->daLoop);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "localNumberOfObservations") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->localNumberOfObservations);
        else
            strcpy(val, NOT_FOUND);
    }
    else if (strcmp(key, "satelliteID") == 0) {
        if (isEcmwfLocal)
            *len = snprintf(val, 32, "%ld", bh->satelliteID);
        else
            strcpy(val, NOT_FOUND);
    }

    else if (strcmp(key, "numberOfSubsets") == 0)
        *len = snprintf(val, 32, "%lu", bh->numberOfSubsets);
    else if (strcmp(key, "observedData") == 0)
        *len = snprintf(val, 32, "%ld", bh->observedData);
    else if (strcmp(key, "compressedData") == 0)
        *len = snprintf(val, 32, "%ld", bh->compressedData);
    else
        return GRIB_NOT_FOUND;

    return GRIB_SUCCESS;
}

// Returns 1 if the BUFR key is in the header and 0 if it is in the data section
int codes_bufr_key_is_header(const grib_handle* h, const char* key, int* err)
{
    const grib_accessor* acc = grib_find_accessor(h, key);
    if (!acc) {
        *err = GRIB_NOT_FOUND;
        return 0;
    }
    *err = GRIB_SUCCESS;
    return ((acc->flags_ & GRIB_ACCESSOR_FLAG_BUFR_DATA) == 0);
}

// Returns 1 if the BUFR key is a coordinate descriptor
int codes_bufr_key_is_coordinate(const grib_handle* h, const char* key, int* err)
{
    const grib_accessor* acc = grib_find_accessor(h, key);
    if (!acc) {
        *err = GRIB_NOT_FOUND;
        return 0;
    }
    *err = GRIB_SUCCESS;
    return ((acc->flags_ & GRIB_ACCESSOR_FLAG_BUFR_COORD) != 0);
}

int codes_bufr_key_exclude_from_dump(const char* key)
{
    if (strstr(key, "percentConfidence->percentConfidence->percentConfidence->percentConfidence->percentConfidence")) {
        return 1;
    }
    return 0;
}
