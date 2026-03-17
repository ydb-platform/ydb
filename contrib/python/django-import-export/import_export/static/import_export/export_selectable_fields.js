function hideUnselectedResourceFields(selectedResourceIndex) {
  const fields = document.querySelectorAll("[resource-index]");

  fields.forEach((field) => {
    if (field.getAttribute("resource-index") !== selectedResourceIndex.toString()) {
      // field is wrapped by div, change visibility on wrapper
      field.style.display = "none";
    }
  });
}

function showSelectedResourceFields(resourceIndex) {
  const fields = document.querySelectorAll(`[resource-index="${resourceIndex}"]`);

  fields.forEach((field) => {
    // field is wrapped by div, change visibility on wrapper
    field.style.display = "block";
  });
}

function onResourceSelected(e) {
  const resourceIndex = e.target.value;

  showSelectedResourceFields(resourceIndex);

  hideUnselectedResourceFields(resourceIndex);
}

function onSelectToggleChange(e) {
  /*
  * Handles a checkbox click event to select / deselect all field checkboxes.
  */ 
  const select = e.target;
  const isChecked = select.checked;
  
  if (isChecked) {
    document.querySelectorAll('.selectable-field-export-row input[type="checkbox"]').forEach((checkbox) => {
        checkbox.checked = true;
    });
  } else {
    document.querySelectorAll('.selectable-field-export-row input[type="checkbox"]').forEach((checkbox) => {
        checkbox.checked = false;
    });
  }
}

document.addEventListener("DOMContentLoaded", () => {
  const resourceSelector = document.querySelector("#id_resource");

  if (!resourceSelector) {
    console.error("resource select input not found");
    return;
  }

  // If selector is actually select input, get selected option.
  // else selected resource index is 0
  const selectedResourceIndex = resourceSelector.tagName === "SELECT" ? resourceSelector.value : 0;

  resourceSelector.addEventListener("input", onResourceSelected);

  // initially hide unselected resource fields
  hideUnselectedResourceFields(selectedResourceIndex);
});
