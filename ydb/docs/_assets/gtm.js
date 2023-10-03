// Define dataLayer and the gtag function.
window.dataLayer = window.dataLayer || [];
function gtag(){dataLayer.push(arguments);}

// Default analytics_storage to 'denied'.
window.gtag = window.gtag || gtag;

const hasAnalyticsConsent = window?.localStorage.getItem('hasAnalyticsConsent');

window.gtag('consent', 'default', {
    'analytics_storage': hasAnalyticsConsent === 'true' ? 'granted' : 'denied',
    'wait_for_update': hasAnalyticsConsent === 'true' ? 0 : Infinity,
});

dataLayer.push({
    'event': 'default_consent'
});

function loadGtm(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);};

loadGtm(window, document, 'script', 'dataLayer', 'GTM-W7ZBL4X')
