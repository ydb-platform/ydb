function successAlert(msg) {
    $("#alertContainer").empty();
    $(`<div
    class="alert alert-success alert-dismissible m-0"
    role="alert"
  >
    <div class="d-flex">
      <div>
        <!-- Download SVG icon from http://tabler-icons.io/i/check -->
        <svg xmlns="http://www.w3.org/2000/svg" class="icon alert-icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M5 12l5 5l10 -10" /></svg>
      </div>
      <div>${msg}</div>
    </div>
    <a class="btn-close" data-bs-dismiss="alert" aria-label="close"></a>
  </div>
  `).appendTo("#alertContainer");
}

function dangerAlert(msg) {
    $("#alertContainer").empty();
    $(`<div
    class="alert alert-danger alert-dismissible m-0"
    role="alert"
  >
    <div class="d-flex">
      <div>
        <!-- Download SVG icon from http://tabler-icons.io/i/check -->
        <svg xmlns="http://www.w3.org/2000/svg" class="icon alert-icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><circle cx="12" cy="12" r="9" /><line x1="12" y1="8" x2="12.01" y2="8" /><polyline points="11 12 12 12 12 16 13 16" /></svg>
     </div>
      <div>${msg}</div>
    </div>
    <a class="btn-close" data-bs-dismiss="alert" aria-label="close"></a>
  </div>
  `).appendTo("#alertContainer");
}

if (localStorage.successAlert){
    successAlert(localStorage.successAlert)
    localStorage.removeItem('successAlert')
}

if (localStorage.dangerAlert){
  successAlert(localStorage.dangerAlert)
  localStorage.removeItem('dangerAlert')
}
