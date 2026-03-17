window.addEventListener('load', function () {
  const inputs = document.getElementsByClassName('colorfield_field coloris');
  for (const input of inputs) {
    const colorisId = input.getAttribute('data-coloris-options-json-script-id');
    const script = document.querySelector(`script[id='${colorisId}']`);
    const options = JSON.parse(script.textContent);

    const id = input.getAttribute('id');
    Coloris.setInstance(`.colorfield_field.coloris.${id}`, options);
  }
});
