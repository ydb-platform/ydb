/* global django, console, showRelatedObjectLookupPopup */

/**
 * Overwrite Django's `dismissRelatedLookupPopup` to trigger
 * a change event on the value change, so dynamic_raw_id can
 * catch it and update the associated label.
 */
if (!windowname_to_id) {
  function windowname_to_id(text) {
    return text
      .replace(/__dot__/g, '.')
      .replace(/__dash__/g, '-')
      .replace(/__\d+$/, '');
  }
}

function dismissRelatedLookupPopup(win, chosenId) {
  const name = windowname_to_id(win.name);
  const elem = document.getElementById(name);
  if (elem.className.indexOf('vManyToManyRawIdAdminField') !== -1 && elem.value) {
    elem.value += `,${chosenId}`;
  } else {
    elem.value = chosenId;
  }
  django.jQuery(elem).trigger('change');
  win.close();
}

(function($) {
  $(document).ready(function($) {

    function update_dynamic_raw_id_label(element, multi) {
      let name = element.next('a').attr('data-name');
      const app = element.next('a').attr('data-app');
      const model = element.next('a').attr('data-model');
      const value = element.val();

      const ADMIN_URL = window.DYNAMIC_RAW_ID_MOUNT_URL || '/admin/';
      const MOUNT_URL = `${ADMIN_URL}dynamic_raw_id`;

      let url = `${MOUNT_URL}/${app}/${model}/`;

      if (multi === true) {
        url = `${url}multiple/`;
      }

      try {
        // only fire the ajax call if we have all the required info
        if (
          (name !== undefined) &&
          (value !== undefined) &&
          (value !== '')
        ) {
          // Handles elements added via the TabularInline add row functionality
          if (name.search(/__prefix__/) !== -1) {
            name = element.attr('id').replace('id_', '');
          }

          $.ajax({
            url: url,
            data: {'id': value},
            success: function(data) {
              $(`#${name}_dynamic_raw_id_label`).html(` ${data}`);
            }
          });
        }
      } catch (e) {
        console.log('Oups, we have a problem' + e);
      }
    }

    $('.vForeignKeyRawIdAdminField').change(function(e) {
      update_dynamic_raw_id_label($(this), false);
      e.stopPropagation();
    });

    // Handle ManyToManyRawIdAdminFields.
    $('.vManyToManyRawIdAdminField').change(function(e) {
      update_dynamic_raw_id_label($(this), true);
      e.stopPropagation();
    });

    // Clear both the input field and the labels
    $('.dynamic_raw_id-clear-field').click(function() {
      const $this = $(this);
      $this
        .parent()
        .find('.vForeignKeyRawIdAdminField, .vManyToManyRawIdAdminField')
        .val('')
        .trigger('change');

      $this
        .closest('.dynamic_raw_id_label')
        .html('&nbsp;');
    });

    // Open up the pop up window and set the focus in the input field
    $('.dynamic_raw_id-related-lookup').click(function() {
      // Actual Django javascript function
      showRelatedObjectLookupPopup(this);

      // Set the focus into the input field
      $(this).parent().find('input').focus();

      return false;
    });

    // Update the dynamic_raw_id fields on loads
    $('.vManyToManyRawIdAdminField').each(function() {
      update_dynamic_raw_id_label($(this), true);
    });
    $('.vForeignKeyRawIdAdminField').each(function() {
      update_dynamic_raw_id_label($(this), false);
    });

  });
})(django.jQuery);
