extern crate winapi;

use super::{LanguageRange,Locale};

use std::fmt::Write;

fn if_locale_info_differs<F: FnOnce(&str)>(lc_type: winapi::ctypes::c_ulong, func: F) {
    #[allow(non_snake_case)] // would be const if it wasn't for the fact const functions are still unstable
    let LOCALE_NAME_USER_DEFAULT: *mut u16 = ::std::ptr::null_mut();
    const LOCALE_NOUSEROVERRIDE: winapi::ctypes::c_ulong = 0x80000000;
    let mut buf_user = [0u16; 86];
    let mut buf_def = [0u16; 86];
    let len_user = unsafe {
        winapi::um::winnls::GetLocaleInfoEx(LOCALE_NAME_USER_DEFAULT, lc_type,
                                  buf_user.as_mut_ptr(), buf_user.len() as winapi::ctypes::c_long)
    };
    let len_def = unsafe {
        winapi::um::winnls::GetLocaleInfoEx(LOCALE_NAME_USER_DEFAULT, lc_type | LOCALE_NOUSEROVERRIDE,
                                  buf_def.as_mut_ptr(), buf_user.len() as winapi::ctypes::c_long)
    };
    if buf_user[0..(len_user as usize - 1)] != buf_def[0..(len_def as usize - 1)] {
        let s = &*String::from_utf16_lossy(&buf_user[0..(len_user as usize - 1)]);
        func(&*s);
    }
}

#[allow(dead_code)]
mod consts {
    use super::winapi::ctypes::c_ulong;
    // Locale information types from winnls.h
    pub const LOCALE_ILANGUAGE: c_ulong =              0x0001;
    pub const LOCALE_SLANGUAGE: c_ulong =              0x0002;
    pub const LOCALE_SENGLANGUAGE: c_ulong =           0x1001;
    pub const LOCALE_SENGLISHLANGUAGENAME: c_ulong =   0x1001;
    pub const LOCALE_SABBREVLANGNAME: c_ulong =        0x0003;
    pub const LOCALE_SNATIVELANGNAME: c_ulong =        0x0004;
    pub const LOCALE_SNATIVELANGUAGENAME: c_ulong =    0x0004;
    pub const LOCALE_ICOUNTRY: c_ulong =               0x0005;
    pub const LOCALE_SCOUNTRY: c_ulong =               0x0006;
    pub const LOCALE_SLOCALIZEDCOUNTRYNAME: c_ulong =  0x0006;
    pub const LOCALE_SENGCOUNTRY: c_ulong =            0x1002;
    pub const LOCALE_SENGLISHCOUNTRYNAME: c_ulong =    0x1002;
    pub const LOCALE_SABBREVCTRYNAME: c_ulong =        0x0007;
    pub const LOCALE_SNATIVECTRYNAME: c_ulong =        0x0008;
    pub const LOCALE_SNATIVECOUNTRYNAME: c_ulong =     0x0008;
    pub const LOCALE_IDEFAULTLANGUAGE: c_ulong =       0x0009;
    pub const LOCALE_IDEFAULTCOUNTRY: c_ulong =        0x000A;
    pub const LOCALE_IDEFAULTCODEPAGE: c_ulong =       0x000B;
    pub const LOCALE_IDEFAULTANSICODEPAGE: c_ulong =   0x1004;
    pub const LOCALE_IDEFAULTMACCODEPAGE: c_ulong =    0x1011;
    pub const LOCALE_SLIST: c_ulong =                  0x000C;
    pub const LOCALE_IMEASURE: c_ulong =               0x000D;
    pub const LOCALE_SDECIMAL: c_ulong =               0x000E;
    pub const LOCALE_STHOUSAND: c_ulong =              0x000F;
    pub const LOCALE_SGROUPING: c_ulong =              0x0010;
    pub const LOCALE_IDIGITS: c_ulong =                0x0011;
    pub const LOCALE_ILZERO: c_ulong =                 0x0012;
    pub const LOCALE_INEGNUMBER: c_ulong =             0x1010;
    pub const LOCALE_SNATIVEDIGITS: c_ulong =          0x0013;
    pub const LOCALE_SCURRENCY: c_ulong =              0x0014;
    pub const LOCALE_SINTLSYMBOL: c_ulong =            0x0015;
    pub const LOCALE_SMONDECIMALSEP: c_ulong =         0x0016;
    pub const LOCALE_SMONTHOUSANDSEP: c_ulong =        0x0017;
    pub const LOCALE_SMONGROUPING: c_ulong =           0x0018;
    pub const LOCALE_ICURRDIGITS: c_ulong =            0x0019;
    pub const LOCALE_IINTLCURRDIGITS: c_ulong =        0x001A;
    pub const LOCALE_ICURRENCY: c_ulong =              0x001B;
    pub const LOCALE_INEGCURR: c_ulong =               0x001C;
    pub const LOCALE_SDATE: c_ulong =                  0x001D;
    pub const LOCALE_STIME: c_ulong =                  0x001E;
    pub const LOCALE_SSHORTDATE: c_ulong =             0x001F;
    pub const LOCALE_SLONGDATE: c_ulong =              0x0020;
    pub const LOCALE_STIMEFORMAT: c_ulong =            0x1003;
    pub const LOCALE_IDATE: c_ulong =                  0x0021;
    pub const LOCALE_ILDATE: c_ulong =                 0x0022;
    pub const LOCALE_ITIME: c_ulong =                  0x0023;
    pub const LOCALE_ITIMEMARKPOSN: c_ulong =          0x1005;
    pub const LOCALE_ICENTURY: c_ulong =               0x0024;
    pub const LOCALE_ITLZERO: c_ulong =                0x0025;
    pub const LOCALE_IDAYLZERO: c_ulong =              0x0026;
    pub const LOCALE_IMONLZERO: c_ulong =              0x0027;
    pub const LOCALE_S1159: c_ulong =                  0x0028;
    pub const LOCALE_S2359: c_ulong =                  0x0029;
    pub const LOCALE_ICALENDARTYPE: c_ulong =          0x1009;
    pub const LOCALE_IOPTIONALCALENDAR: c_ulong =      0x100B;
    pub const LOCALE_IFIRSTDAYOFWEEK: c_ulong =        0x100C;
    pub const LOCALE_IFIRSTWEEKOFYEAR: c_ulong =       0x100D;
    pub const LOCALE_SDAYNAME1: c_ulong =              0x002A;
    pub const LOCALE_SDAYNAME2: c_ulong =              0x002B;
    pub const LOCALE_SDAYNAME3: c_ulong =              0x002C;
    pub const LOCALE_SDAYNAME4: c_ulong =              0x002D;
    pub const LOCALE_SDAYNAME5: c_ulong =              0x002E;
    pub const LOCALE_SDAYNAME6: c_ulong =              0x002F;
    pub const LOCALE_SDAYNAME7: c_ulong =              0x0030;
    pub const LOCALE_SABBREVDAYNAME1: c_ulong =        0x0031;
    pub const LOCALE_SABBREVDAYNAME2: c_ulong =        0x0032;
    pub const LOCALE_SABBREVDAYNAME3: c_ulong =        0x0033;
    pub const LOCALE_SABBREVDAYNAME4: c_ulong =        0x0034;
    pub const LOCALE_SABBREVDAYNAME5: c_ulong =        0x0035;
    pub const LOCALE_SABBREVDAYNAME6: c_ulong =        0x0036;
    pub const LOCALE_SABBREVDAYNAME7: c_ulong =        0x0037;
    pub const LOCALE_SMONTHNAME1: c_ulong =            0x0038;
    pub const LOCALE_SMONTHNAME2: c_ulong =            0x0039;
    pub const LOCALE_SMONTHNAME3: c_ulong =            0x003A;
    pub const LOCALE_SMONTHNAME4: c_ulong =            0x003B;
    pub const LOCALE_SMONTHNAME5: c_ulong =            0x003C;
    pub const LOCALE_SMONTHNAME6: c_ulong =            0x003D;
    pub const LOCALE_SMONTHNAME7: c_ulong =            0x003E;
    pub const LOCALE_SMONTHNAME8: c_ulong =            0x003F;
    pub const LOCALE_SMONTHNAME9: c_ulong =            0x0040;
    pub const LOCALE_SMONTHNAME10: c_ulong =           0x0041;
    pub const LOCALE_SMONTHNAME11: c_ulong =           0x0042;
    pub const LOCALE_SMONTHNAME12: c_ulong =           0x0043;
    pub const LOCALE_SMONTHNAME13: c_ulong =           0x100E;
    pub const LOCALE_SABBREVMONTHNAME1: c_ulong =      0x0044;
    pub const LOCALE_SABBREVMONTHNAME2: c_ulong =      0x0045;
    pub const LOCALE_SABBREVMONTHNAME3: c_ulong =      0x0046;
    pub const LOCALE_SABBREVMONTHNAME4: c_ulong =      0x0047;
    pub const LOCALE_SABBREVMONTHNAME5: c_ulong =      0x0048;
    pub const LOCALE_SABBREVMONTHNAME6: c_ulong =      0x0049;
    pub const LOCALE_SABBREVMONTHNAME7: c_ulong =      0x004A;
    pub const LOCALE_SABBREVMONTHNAME8: c_ulong =      0x004B;
    pub const LOCALE_SABBREVMONTHNAME9: c_ulong =      0x004C;
    pub const LOCALE_SABBREVMONTHNAME10: c_ulong =     0x004D;
    pub const LOCALE_SABBREVMONTHNAME11: c_ulong =     0x004E;
    pub const LOCALE_SABBREVMONTHNAME12: c_ulong =     0x004F;
    pub const LOCALE_SABBREVMONTHNAME13: c_ulong =     0x100F;
    pub const LOCALE_SPOSITIVESIGN: c_ulong =          0x0050;
    pub const LOCALE_SNEGATIVESIGN: c_ulong =          0x0051;
    pub const LOCALE_IPOSSIGNPOSN: c_ulong =           0x0052;
    pub const LOCALE_INEGSIGNPOSN: c_ulong =           0x0053;
    pub const LOCALE_IPOSSYMPRECEDES: c_ulong =        0x0054;
    pub const LOCALE_IPOSSEPBYSPACE: c_ulong =         0x0055;
    pub const LOCALE_INEGSYMPRECEDES: c_ulong =        0x0056;
    pub const LOCALE_INEGSEPBYSPACE: c_ulong =         0x0057;
    pub const LOCALE_FONTSIGNATURE: c_ulong =          0x0058;
    pub const LOCALE_SISO639LANGNAME: c_ulong =        0x0059;
    pub const LOCALE_SISO3166CTRYNAME: c_ulong =       0x005A;
    pub const LOCALE_IGEOID: c_ulong =                 0x005B;
    pub const LOCALE_SNAME: c_ulong =                  0x005C;
    pub const LOCALE_SDURATION: c_ulong =              0x005D;
    pub const LOCALE_SKEYBOARDSTOINSTALL: c_ulong =    0x005E;
    pub const LOCALE_SSHORTESTDAYNAME1: c_ulong =      0x0060;
    pub const LOCALE_SSHORTESTDAYNAME2: c_ulong =      0x0061;
    pub const LOCALE_SSHORTESTDAYNAME3: c_ulong =      0x0062;
    pub const LOCALE_SSHORTESTDAYNAME4: c_ulong =      0x0063;
    pub const LOCALE_SSHORTESTDAYNAME5: c_ulong =      0x0064;
    pub const LOCALE_SSHORTESTDAYNAME6: c_ulong =      0x0065;
    pub const LOCALE_SSHORTESTDAYNAME7: c_ulong =      0x0066;
    pub const LOCALE_SISO639LANGNAME2: c_ulong =       0x0067;
    pub const LOCALE_SISO3166CTRYNAME2: c_ulong =      0x0068;
    pub const LOCALE_SNAN: c_ulong =                   0x0069;
    pub const LOCALE_SPOSINFINITY: c_ulong =           0x006A;
    pub const LOCALE_SNEGINFINITY: c_ulong =           0x006B;
    pub const LOCALE_SSCRIPTS: c_ulong =               0x006C;
    pub const LOCALE_SPARENT: c_ulong =                0x006D;
    pub const LOCALE_SCONSOLEFALLBACKNAME: c_ulong =   0x006E;
    pub const LOCALE_SLANGDISPLAYNAME: c_ulong =       0x006F;
    pub const LOCALE_SLOCALIZEDLANGUAGENAME: c_ulong = 0x006F;
    pub const LOCALE_IREADINGLAYOUT: c_ulong =         0x0070;
    pub const LOCALE_INEUTRAL: c_ulong =               0x0071;
    pub const LOCALE_SENGLISHDISPLAYNAME: c_ulong =    0x0072;
    pub const LOCALE_SNATIVEDISPLAYNAME: c_ulong =     0x0073;
    pub const LOCALE_INEGATIVEPERCENT: c_ulong =       0x0074;
    pub const LOCALE_IPOSITIVEPERCENT: c_ulong =       0x0075;
    pub const LOCALE_SPERCENT: c_ulong =               0x0076;
    pub const LOCALE_SPERMILLE: c_ulong =              0x0077;
    pub const LOCALE_SMONTHDAY: c_ulong =              0x0078;
    pub const LOCALE_SSHORTTIME: c_ulong =             0x0079;
    pub const LOCALE_SOPENTYPELANGUAGETAG: c_ulong =   0x007A;
    pub const LOCALE_SSORTLOCALE: c_ulong =            0x007B;

    pub const LOCALE_IDEFAULTEBCDICCODEPAGE: c_ulong = 0x1012;
    pub const LOCALE_IPAPERSIZE: c_ulong =             0x100A;
    pub const LOCALE_SENGCURRNAME: c_ulong =           0x1007;
    pub const LOCALE_SNATIVECURRNAME: c_ulong =        0x1008;
    pub const LOCALE_SYEARMONTH: c_ulong =             0x1006;
    pub const LOCALE_SSORTNAME: c_ulong =              0x1013;
    pub const LOCALE_IDIGITSUBSTITUTION: c_ulong =     0x1014;
} // mod consts

use self::consts::*;

fn get_user_default_locale() -> super::Result<LanguageRange<'static>> {
    let mut buf = [0u16; 85];
    let len = unsafe {
        winapi::um::winnls::GetUserDefaultLocaleName(buf.as_mut_ptr(), buf.len() as i32)
    };
    if len > 0 {
        let mut s = String::from_utf16_lossy(&buf[..(len as usize - 1)]);

        {
            // First collect parameters that have representation in -u extension as defined in
            // Unicode TR35§3.6:
            let mut u_ext = String::new();

            // cf
            // Negative monetary format, Region and Language/Customize Format/Currency (LOCALE_INEGCURR):
            // We only distinguish between parenthesized format (-u-cf-account) and using minus sign
            // (-u-cf-standard).
            // Note: the formats are numbered in rather random order, so the numbers corresponding to
            // parenthesized formats are 0, 4, 14 and 15.
            if_locale_info_differs(LOCALE_INEGCURR, |s| {
                u_ext.push_str(match s { "0"|"4"|"14"|"15" => "-cf-account", _ => "-cf-standard" });
            });

            // fw
            // Start-of-week, Region and Language/Formats and Region and Language/Customize
            // Formats/Date (LOCALE_IFIRSTDAYOFWEEK):
            if_locale_info_differs(LOCALE_IFIRSTDAYOFWEEK, |s| {
                u_ext.push_str(match s { "0" => "-fw-mon", "1" => "-fw-tue", "2" => "-fw-wed", "3" => "-fw-thu", "4" => "-fw-fri", "5" => "-fw-sat", "6" => "-fw-sun", _ => "" });
            });

            // hc
            // Time formats, Region and Language/Formats and Region and Language/Customize Formats/Time
            // (LOCALE_SSHORTTIME + LOCALE_STIMEFORMAT):
            // We only use the hour-cycle here
            // NOTE: Only ‘h’ and ‘H’ actually appear.
            // TODO: Verify the hour-cycles in Windows actually match the Unicode TR35
            // TODO: Might also detect whether to use leading zeroes.
            if_locale_info_differs(LOCALE_STIMEFORMAT, |s| {
                if s.contains('h') {
                    u_ext.push_str("-hc-h12"); // 1–12
                } else if s.contains('H') {
                    u_ext.push_str("-hc-h23"); // 0–23
                } else if s.contains('K') {
                    u_ext.push_str("-hc-h11"); // 0–11
                } else if s.contains('k') {
                    u_ext.push_str("-hc-h24"); // 1–24
                }
            });

            // ms
            // Measurement system, Region and Language/Customize Format/Numbers:
            if_locale_info_differs(LOCALE_IMEASURE, |s| {
                u_ext.push_str(match s { "0" => "-ms-metric", "1" => "-ms-ussystem", _ => "" });
            });

            // nu
            // Standard digits, Region and Language/Customize Format/Numbers:
            // TODO: We should probably take into account the LOCALE_IDIGITSUBSTITUTION and set the
            // digits if either LOCALE_SNATIVEDIGITS or LOCALE_IDIGITSUBSTITUTION is not default AND
            // LOCALE_IDIGITSUBSTITUTION is not 1 (0 = contextual, 1 = use latin, 2 = use specified).
            // I am not sure we will implement the contextual logic too.
            if_locale_info_differs(LOCALE_SNATIVEDIGITS, |s| {
                u_ext.push_str(match s.as_ref() {
                    // basic plane numeric numberingSystems from CLDR
                    "٠١٢٣٤٥٦٧٨٩" => "-nu-arab",
                    "۰۱۲۳۴۵۶۷۸۹" => "-nu-arabext",
                    "᭐᭑᭒᭓᭔᭕᭖᭗᭘᭙" => "-nu-bali",
                    "০১২৩৪৫৬৭৮৯" => "-nu-beng",
                    "꩐꩑꩒꩓꩔꩕꩖꩗꩘꩙" => "-nu-cham",
                    "०१२३४५६७८९" => "-nu-deva",
                    "０１２３４５６７８９" => "-nu-fullwide",
                    "૦૧૨૩૪૫૬૭૮૯" => "-nu-gujr",
                    "੦੧੨੩੪੫੬੭੮੯" => "-nu-guru",
                    "〇一二三四五六七八九" => "-nu-hanidec",
                    "꧐꧑꧒꧓꧔꧕꧖꧗꧘꧙" => "-nu-java",
                    "꤀꤁꤂꤃꤄꤅꤆꤇꤈꤉" => "-nu-kali",
                    "០១២៣៤៥៦៧៨៩" => "-nu-khmr",
                    "೦೧೨೩೪೫೬೭೮೯" => "-nu-knda",
                    "᪀᪁᪂᪃᪄᪅᪆᪇᪈᪉" => "-nu-lana",
                    "᪐᪑᪒᪓᪔᪕᪖᪗᪘᪙" => "-nu-lanatham",
                    "໐໑໒໓໔໕໖໗໘໙" => "-nu-laoo",
                    "0123456789" => "-nu-latn",
                    "᱀᱁᱂᱃᱄᱅᱆᱇᱈᱉" => "-nu-lepc",
                    "᥆᥇᥈᥉᥊᥋᥌᥍᥎᥏" => "-nu-limb",
                    "൦൧൨൩൪൫൬൭൮൯" => "-nu-mlym",
                    "᠐᠑᠒᠓᠔᠕᠖᠗᠘᠙" => "-nu-mong",
                    "꯰꯱꯲꯳꯴꯵꯶꯷꯸꯹" => "-nu-mtei",
                    "၀၁၂၃၄၅၆၇၈၉" => "-nu-mymr",
                    "႐႑႒႓႔႕႖႗႘႙" => "-nu-mymrshan",
                    "߀߁߂߃߄߅߆߇߈߉" => "-nu-nkoo",
                    "᱐᱑᱒᱓᱔᱕᱖᱗᱘᱙" => "-nu-olck",
                    "୦୧୨୩୪୫୬୭୮୯" => "-nu-orya",
                    "꣐꣑꣒꣓꣔꣕꣖꣗꣘꣙" => "-nu-saur",
                    "᮰᮱᮲᮳᮴᮵᮶᮷᮸᮹" => "-nu-sund",
                    "᧐᧑᧒᧓᧔᧕᧖᧗᧘᧙" => "-nu-talu",
                    "௦௧௨௩௪௫௬௭௮௯" => "-nu-tamldec",
                    "౦౧౨౩౪౫౬౭౮౯" => "-nu-telu",
                    "๐๑๒๓๔๕๖๗๘๙" => "-nu-thai",
                    "༠༡༢༣༤༥༦༧༨༩" => "-nu-tibt",
                    "꘠꘡꘢꘣꘤꘥꘦꘧꘨꘩" => "-nu-vaii",
                    // I don't think Windows can configure anything else, but just in case
                    _ => "",
                })
            });

            // Append the extensions, if any
            if !u_ext.is_empty() {
                s.push_str("-u");
                s.push_str(&*u_ext);
            }
        }
        {
            // Then collect other parameters that we want to represent using custom -x extension.
            let mut x_ext = String::new();

            // df
            // Date formats, Region and Language/Formats and Region and Language/Customize
            // Formats/Date (LOCALE_SSHORTDATE + LOCALE_SLONGDATE):
            // The full format is too long and complex for the tag format, so we only interpret ISO
            // format. TODO: might also detect swapped order and perhaps abbrev name vs. number for
            // short format.
            if_locale_info_differs(LOCALE_SSHORTDATE, |s| {
                if s == "yyyy-MM-dd" {
                    x_ext.push_str("-df-iso")
                }
            });

            // dm
            // Monetary decimal separator, Region and Language/Customize Format/Currency:
            if_locale_info_differs(LOCALE_SMONDECIMALSEP, |s| {
                x_ext.push_str("-dm");
                for c in s.chars() {
                    write!(&mut x_ext, "-{:04x}", c as u32).unwrap(); // shouldn't fail unless OOM
                }
            });

            // ds
            // Decimal separator, Region and Language/Customize Format/Numbers:
            if_locale_info_differs(LOCALE_SDECIMAL, |s| {
                x_ext.push_str("-ds");
                for c in s.chars() {
                    write!(&mut x_ext, "-{:04x}", c as u32).unwrap(); // shouldn't fail unless OOM
                }
            });

            // gm
            // Monetary group separator, Region and Language/Customize Format/Currency:
            if_locale_info_differs(LOCALE_SMONTHOUSANDSEP, |s| {
                x_ext.push_str("-gm");
                for c in s.chars() {
                    write!(&mut x_ext, "-{:04x}", c as u32).unwrap(); // shouldn't fail unless OOM
                }
            });

            // gs
            // Group separator, Region and Language/Customize Format/Numbers:
            if_locale_info_differs(LOCALE_STHOUSAND, |s| {
                x_ext.push_str("-gs");
                for c in s.chars() {
                    write!(&mut x_ext, "-{:04x}", c as u32).unwrap(); // shouldn't fail unless OOM
                }
            });

            // ls
            // List separator, Region and Language/Customize Format/Numbers:
            if_locale_info_differs(LOCALE_SLIST, |s| {
                x_ext.push_str("-ls");
                for c in s.chars() {
                    write!(&mut x_ext, "-{:04x}", c as u32).unwrap(); // shouldn't fail unless OOM
                }
            });

            // Append the extensions, if any
            if !x_ext.is_empty() {
                s.push_str("-x");
                s.push_str(&*x_ext);
            }
        }

        // TODO: Below are the settings we don't handle, but might want to.
        // ** Region and Language/Location:
        //  - TODO: Location - LOCALE_IGEOID does not seem to indicate it; not sure what does.
        // ** Region and Language/Customize Format/Numbers:
        //  - TODO: Grouping — LOCALE_SGROUPING — not sure how much I really want to deal with
        //    this.
        //  - TODO: Negative sign — LOCALE_SNEGATIVESIGN — not sure this makes sense. CLDR has
        //    U+002D HYPHEN-MINUS almost everywhere with some U+2212 MINUS SIGN thrown in the mix
        //    rather randomly, one clearly nonsensical U+2013 EN DASH and handful of RTL/LTR marks
        //    that we should use according to script (or not at all; sure digits and +/- have sane
        //    RTL flags).
        //  - NOTE: Positive sign does not seem to be configurable and for en-GB even returns
        //    somewhat absurd value of “”.
        //  - TODO: Negative number format — Unicode TR35 only has this for currency (separate
        //    setting) and CLDR does not have any exceptions from the default of prefix-with-minus
        //    for decimal formats (only for currency formats).
        //  - TODO: Leading zeroes — LOCALE_ILZERO — whether to use .7 or 0.7.
        // ** Region and Language/Customize Format/Currency:
        //  - TODO: Positive monetary format — LOCALE_ICURRENCY: Currency symbol can precede or
        //    succeed the value and can be separated by space or not, but we need to design
        //    representation for it in the -x extension space.
        //  - TODO: Negative monetary format — LOCALE_INEGCURR: We only handle distinction between
        //    parenthesized and regular with minus sign, so possibly design representation for the
        //    other options too.
        //  - TODO: Monetary grouping — LOCALE_SGROUPING — like grouping above, not sure how much
        //    I really want to deal with this.
        // ** Region and Language/Customize Format/Time:
        //  - TODO: Possibly handle other kinds of formats than just distinguishing 12 and 23 hour
        //    formats we do above.
        //  - TODO: Custom AM and PM flags.
        // ** Region and Language/Customize Format/Date:
        //  - TODO: Possibly handle other kinds of formats than just distinguishing the ISO-8601
        //    one we do above.
        //  - Note: Windows don't seem to support any other calendar than Gregorian; makes it
        //    easier for us.

        // NOTE: Below are the settings we don't handle and I don't think we should.
        // ** Region and Language/Customize Format/Numbers:
        //  - Number of digits after decimal — not used! For general numbers it indicates accuracy,
        //    so it must be application's choice and for monetary we always have explicit currency
        //    and use appropriate value for that.
        // ** Region and Language/Customize Format/Currency:
        //  - Currency symbol — since there is no way to set the actual currency, just the symbol,
        //    we can't use it as our monetary formatting is based on explicit currency.
        //  - Monetary decimal digits — we always use the value for specified currency, not the
        //    local value.

        return LanguageRange::new(&*s).map(|x| x.into_static());
    }
    // TODO: Fall back to GetUserDefaultLCID and/or GetLocaleInfoW
    // GetLocaleInfo(Ex) with LOCALE_SISO639LANGNAME and LOCALE_SISO3166CTRYNAME might be of some
    // utility too
    return Err(super::Error::NotWellFormed);
}

fn get_system_default_locale() -> super::Result<LanguageRange<'static>> {
    let mut buf = [0u16; 85];
    let len = unsafe {
        winapi::um::winnls::GetSystemDefaultLocaleName(buf.as_mut_ptr(), buf.len() as i32)
    };
    if len > 0 {
        let s = String::from_utf16_lossy(&buf[..(len as usize - 1)]);
        return LanguageRange::new(&*s).map(|x| x.into_static());
    }
    // TODO: Fall back to GetUserDefaultLCID and/or GetLocaleInfoW
    // GetLocaleInfo(Ex) with LOCALE_SISO639LANGNAME and LOCALE_SISO3166CTRYNAME might be of some
    // utility too
    return Err(super::Error::NotWellFormed);
}

const MUI_LANGUAGE_NAME: winapi::ctypes::c_ulong = 0x8; // from winnls.h

fn get_user_preferred_languages() -> Vec<LanguageRange<'static>> {
    let mut buf = [0u16; 5 * 85 + 1];
    let mut n_langs = 0;
    let mut len = buf.len() as winapi::ctypes::c_ulong;
    let res = unsafe {
        winapi::um::winnls::GetUserPreferredUILanguages(MUI_LANGUAGE_NAME, &mut n_langs, buf.as_mut_ptr(), &mut len)
    };
    if res != 0 && len > 1 {
        let s = String::from_utf16_lossy(&buf[..(len as usize - 2)]);
        return s.split('\0')
            .filter_map(|x| LanguageRange::new(x).ok())
            .map(|x| x.into_static())
            .collect();
    }
    return Vec::new();
}

// User default language is the primary language. If user preferred UI languages returns anything, the
// first item is considered override for messages and the rest is fallbacks. And last fallback is
// system default language.
pub fn system_locale() -> Option<Locale> {
    let mut loc =
        if let Ok(lr) = get_user_default_locale() {
            Some(Locale::from(lr))
        } else {
            None
        };
    let mut msg_locs = get_user_preferred_languages();
    if !msg_locs.is_empty() {
        if loc.is_none() {
            loc = Some(Locale::from(msg_locs.remove(0)));
        } else {
            loc.as_mut().unwrap().add_category("messages", &msg_locs.remove(0));
        }
        debug_assert!(!loc.is_none());
        for l in msg_locs {
            loc.as_mut().unwrap().add(&l);
        }
    }
    if let Ok(lr) = get_system_default_locale() {
        if loc.is_none() {
            loc = Some(Locale::from(lr));
        } else {
            loc.as_mut().unwrap().add(&lr);
        }
    }
    loc
}
